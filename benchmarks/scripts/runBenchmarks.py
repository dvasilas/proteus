from __future__ import print_function
import argparse
import subprocess as subprocess
import sys
import os
import os.path
import pickle
import json
import time
from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
STATE = 0

def getCredentials(credFile):
    credentials = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credFile, SCOPES)
            credentials = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)
    return credentials

def pasteToSpreadsheet(credFile, spreadsheet_id, data, metadata):
    credentials = getCredentials('credentials_scality.json')
    service = build('sheets', 'v4', credentials=credentials)
    addSheet_request = {
        'requests': [{ 'addSheet': {
            'properties': {
              'title': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              }
            }
          }]
    }
    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=addSheet_request)
    addSheetResp = request.execute()
    newSheetId = addSheetResp['replies'][0]['addSheet']['properties']['sheetId']
    pasteMetadata_request = {
        'requests': [{
          'pasteData': {
            'coordinate': {
              'sheetId': newSheetId,
              'rowIndex': 0,
              'columnIndex': 0
            },
            'data': metadata,
            'delimiter': ",",
          }
          }]
    }
    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=pasteMetadata_request)
    pasteMetadataResp = request.execute()
    print(pasteMetadataResp)
    pasteData_request = {
        'requests': [{
          'pasteData': {
            'coordinate': {
              'sheetId': newSheetId,
              'rowIndex': 0,
              'columnIndex': 2
            },
            'data': data,
            'delimiter': ",",
          }
          }]
    }
    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=pasteData_request)
    pasteData = request.execute()
    print(pasteData)

def parseFile(path):
  output = ''
  with open(path) as fp:
    overallMeasurements = {}
    queryMeasurements = {}
    line = fp.readline()
    while line:
      line = line.strip().split(", ")
      if line[0] == '[QUERY]':
        queryMeasurements[line[1]] = line[2]
      elif line[0] == '[OVERALL]':
        overallMeasurements[line[1]] = line[2]
      line = fp.readline()
  output += queryMeasurements['Throughput(ops/sec)'] + ", "
  output += str(float(queryMeasurements['AverageLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['MinLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['MaxLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['95thPercentileLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['99thPercentileLatency(us)']) / 1000)
  return output

def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parse', action='store_true')
  parser.add_argument('--dest')
  parser.add_argument('--config')
  parser.add_argument('--mapping')
  parser.add_argument('--cred')
  parser.add_argument('--spreadid')
  args = parser.parse_args()
  if not os.path.isabs(args.dest):
    args.dest = os.path.join(os.path.abspath(os.path.dirname(__file__)), args.dest)
  return args

def getLatestCommitTag(project, branch, log):
  output = runCmd(['cd %s' % (project) + ' && git pull origin %s' % (branch) + ' && git log -1 --pretty=%H'], log)
  output = output.split('\n')
  return output[len(output)-2][0:8]

def createOverlayNetwork(log):
  global STATE
  runCmd(['docker network create -d overlay --attachable proteus_net'], log, True)

def waitTermination(services, log):
  for service in services:
    complete = False
    while not complete:
      result = runCmd(['docker service ps %s' % (service)
      + " --format '{{.CurrentState}}'"], log, showProgress=True)
      if result.split(' ')[0] == 'Complete':
        complete = True
      else:
        time.sleep(1)

def runCmd(cmd, log, okToFail=False, showProgress=False):
  if not showProgress:
    print(cmd)
  p = subprocess.Popen(cmd, stdout = subprocess.PIPE, universal_newlines=True, shell=True)
  output = ''
  for stdout_line in iter(p.stdout.readline, ""):
    if showProgress:
      sys.stdout.write('\r' + " " * 100)
      sys.stdout.flush()
      sys.stdout.write('\r' + stdout_line[:-1])
      sys.stdout.flush()
    else:
      print(stdout_line[:-1])
    if not log == None:
      log.write(stdout_line)
    output += stdout_line
  p.stdout.close()
  returnCode = p.wait()
  if returnCode and not okToFail:
      print("error\n%s\nreturn code: %d" % (cmd, returnCode))
      cleanup(log, 0)
      sys.exit()
  return output

def deploy(target, recordCount, systemTag, composeFile, placement, nodeLabels, log):
  global STATE
  cacheSize = recordCount * 5 * 0.2
  runCmd(['env PROTEUS_IMAGE_TAG=%s ' % (systemTag)
    + 'env CACHE_SIZE=%d ' % (cacheSize)
    + 'docker stack deploy '
    + '--compose-file proteus/deployment/compose-files/%s %s' % (composeFile, target)], log)
  STATE += 1
  checkPlacement(target, placement, nodeLabels, log)

def checkPlacement(target, placement, nodeLabels, log):
  for service in placement:
    if service.startswith(target):
      targetLabel = getattr(placement, service)
      targetNode = getattr(nodeLabels, targetLabel)
      output = ""
      countRetries = 0
      while output.split('\n')[0]  != targetNode:
        output = runCmd(['docker service ps --format "{{ .Node }}" %s' % (service)], log)
        countRetries += 1
        time.sleep(1)
        if countRetries > 10:
          print('placement error: '
            + 'service %s should be deployed on node %s, ' % (service, targetNode)
            + 'but is deployed on node %s' % ( output.split('\n')[0]))
          cleanup(0, log)
          sys.exit()

def cleanup(target, log):
  global STATE
  while STATE > target:
    if STATE == 1:
      runCmd(['docker stack rm storage_engine'], log)
    elif STATE == 2:
      runCmd(['docker stack rm query_engine'], log)
    elif STATE == 3:
      print('state should not be %d' % (STATE))
    STATE -= 1

def makeDirLocalRemote(resultDirPath, remoteNodes):
  cmd = 'mkdir -p %s ' %  (resultDirPath)
  for node in remoteNodes:
    cmd += "&& ssh %s 'mkdir -p %s' " %  (node, resultDirPath)
  return runCmd([cmd], None)

def loadDataset(benchToolTag, recordCount, datasetComposeFile, log):
  insertCount = recordCount
  if '_rb' in datasetComposeFile:
    insertCount = recordCount / 3
  insertStart0 = 0
  insertStart1 = insertCount
  insertStart2 = insertStart1 + insertCount
  runCmd(['env YCSB_IMAGE_TAG=%s env RECORDCOUNT=%s ' % (benchToolTag, recordCount)
    + 'env INSERTSTART0=%s env INSERTSTART1=%s env INSERTSTART2=%s ' % (insertStart0, insertStart1, insertStart2)
    + 'env INSERTCOUNT=%s ' % (insertCount)
    + 'docker stack deploy '
    + '--compose-file proteus/deployment/compose-files/%s ycsb_load' % (datasetComposeFile)
    ], log)
  if '_mb' in datasetComposeFile:
    waitTermination(['ycsb_load_ycsb-0'], log)
  elif '_rb' in datasetComposeFile:
    waitTermination(['ycsb_load_ycsb-0', 'ycsb_load_ycsb-1', 'ycsb_load_ycsb-2'], log)
  runCmd(['docker stack rm ycsb_load'], log)

def createResultDir(resultDirPath):
  if makeDirLocalRemote(resultDirPath, ['dc1_node0', 'dc2_node0']):
    print('could not create result output directories')
    sys.exit()


class BenchmarkSuite(dict):
  def __getattr__(self, key):
    return self[key]
  def __setattr__(self, key, value):
    self[key] = value

  def init(self, resultDirPath, configFileName, (systemRepo, systemBranch), (benchToolRepo, benchToolBranch), nodeLabelFile):
    self.resultDirPath = resultDirPath
    self.configFileName = configFileName
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.log = open(os.path.join(args.dest, 'log_' + self.configFileName + "_" + timestamp), "w")
    self.systemTag = getLatestCommitTag(systemRepo, systemBranch, self.log)
    self.benchToolTag = getLatestCommitTag(benchToolRepo, benchToolBranch, self.log)
    self.nodeLabels = json.loads(nodeLabelFile.read(), object_hook = lambda dict: NodeLabels(dict))

  def createBenchmarks(self):
    self.benchmarks = []
    for config in self.benchmark_configs:
      benchmark = Benchmark(self, config)
      self.benchmarks.append(benchmark)

  def cleanupPreviousRun(self):
    runCmd('docker stack rm query_engine '
    + '&& docker stack rm storage_engine '
    + '&& docker stack rm ycsb_load '
    + '&& docker stack rm ycsb_run ', None, True)

  def initSuite(self):
    createOverlayNetwork(self.log)
    deploy('storage_engine', self.default_config.record_count, self.systemTag, getattr(self.deployment, 'storage_engine'), self.placement, self.nodeLabels, self.log)
    loadDataset(self.benchToolTag, self.default_config.record_count, self.deployment.dataset, self.log)

  def run(self):
    return [ bench.run() for bench in self.benchmarks ]

  def cleanup(self):
    cleanup(0, self.log)

  def benchmarkDescription(self):
    description = 'proteus commit used=%s \n' % (self.systemTag)
    description += 'ycsb commit used=%s \n' % (self.benchToolTag)
    description += 'execution_time=%d \n' % (self.default_config.execution_time)
    description += 'warmup_time=%d \n' % (self.default_config.warmup_time)
    description += 'record_count=%d \n' % (self.default_config.record_count)
    description += 'storage_engine=%s \n' % (self.deployment.storage_engine)
    description += 'query_engine=%s \n' % (self.deployment.query_engine)
    return description


class Benchmark():
  def __init__(self, benchmarkSuite, benchmarkConfig):
    for param in benchmarkSuite.default_config:
      setattr(self, param, benchmarkSuite.default_config[param])
    for param in benchmarkConfig:
      setattr(self, param, benchmarkConfig[param])
    self.resultDirPath = benchmarkSuite.resultDirPath
    self.outputFile = "%dK_%.1f_%.1f_%.1f_%d" % (self.record_count/1000, self.cached_query_proportion, self.query_proportion, self.update_proportion, self.threads)
    self.systemTag = benchmarkSuite.systemTag
    self.benchToolTag = benchmarkSuite.benchToolTag
    self.deployment = benchmarkSuite.deployment
    self.placement = benchmarkSuite.placement
    self.nodeLabels = benchmarkSuite.nodeLabels
    self.log = benchmarkSuite.log

  def run(self):
    deploy('query_engine', self.record_count, self.systemTag, getattr(self.deployment, 'query_engine'), self.placement, self.nodeLabels, self.log)
    insertCount = self.record_count
    if '_rb' in datasetComposeFile:
      insertCount = self.record_count / 3
    insertStart0 = 0
    insertStart1 = insertCount
    insertStart2 = insertStart1 + insertCount
    runCmd(['env YCSB_IMAGE_TAG=%s ' % (self.benchToolTag)
    + 'env PROTEUSHOST=%s env PROTEUSPORT=%d ' % (self.proteus_host, self.proteus_port)
    + 'env RECORDCOUNT=%s ' % (self.record_count)
    + 'env INSERTSTART0=%s env INSERTSTART1=%s env INSERTSTART2=%s ' % (insertStart0, insertStart1, insertStart2)
    + 'env INSERTCOUNT=%s ' % (insertCount)
    + 'env QUERYPROPORTION=%.1f env UPDATEPROPORTION=%.1f ' % (self.query_proportion, self.update_proportion)
    + 'env CACHEDQUERYPROPORTION=%.1f ' % (self.cached_query_proportion)
    + 'env EXECUTIONTIME=%d env WARMUPTIME=%d ' % (self.execution_time, self.warmup_time)
    + 'env THREADS=%d env OUTPUT_FILE_NAME=%s ' % (self.threads, self.outputFile)
    + 'env OUTDIR=%s ' % (self.resultDirPath)
    + 'docker stack deploy '
    + '--compose-file proteus/deployment/compose-files/%s ycsb_run' % (self.deployment.workload)
    ], self.log)
    if '_mb' in self.deployment.workload:
      waitTermination(['ycsb_run_client-0'], self.log)
    elif '_rb' in self.deployment.workload:
      waitTermination(['ycsb_run_client-0', 'ycsb_run_client-1', 'ycsb_run_client-2'], self.log)
    else:
      print('unknown workload compose-file name')
      cleanup(0, self.log)
      sys.exit()
    runCmd(['docker stack rm ycsb_run'], self.log)
    cleanup(1, self.log)
    if '_rb' in getattr(self.deployment, 'query_engine'):
      returnCode = runCmd(['scp dc1_node0:%s/* %s ' %  (self.resultDirPath, self.resultDirPath)
        + '&& scp dc2_node0:%s/* %s' %  (self.resultDirPath, self.resultDirPath)
      ], None)
      if returnCode:
        print('could not fetch results')
        sys.exit()
      returnCode = runCmd(['docker run -v %s:/ycsb ' %  (self.resultDirPath)
        + '-e MEASUREMENT_RESULTS_DIR=/ycsb '
        + '-e PREFIX=%s ' % (self.outputFile)
        + 'dvasilas/ycsb:%s' % (self.benchToolTag+'_parse')
      ], None)
      if returnCode:
        print('could not fetch results')
        sys.exit()

    result = '%.1f, %.1f, %.1f, %d, ' % (self.query_proportion, self.update_proportion, self.cached_query_proportion, self.threads)
    result += parseFile(os.path.join(self.resultDirPath, self.outputFile+'.txt')) + '\n'
    return result

class NodeLabels(dict):
  def __getattr__(self, key):
    return self[key]
  def __setattr__(self, key, value):
    self[key] = value

if __name__ == '__main__':
  args = parseArgs()

  createResultDir(args.dest)
  with open(args.config) as config_file:
    with open(args.mapping) as label_file:
      benchSuite = json.loads(config_file.read(), object_hook = lambda dict: BenchmarkSuite(dict))
      benchSuite.init(args.dest, args.config.split(".")[0], ('proteus', 'benchmarks'), ('YCSB', 'proteus'), label_file)
      benchSuite.cleanupPreviousRun()
      benchSuite.createBenchmarks()
      benchSuite.initSuite()
      results = benchSuite.run()
      data = 'query_proportion, update_proportion, cached_query_proportion, threads, [Q]Throughput(ops/sec), [Q]AverageLatency(ms), [Q]MinLatency(ms), [Q]MaxLatency(ms), [Q]95thPercentileLatency(ms), [Q]99thPercentileLatency(ms) \n'
      for res in results:
        data += res
      data = data[:-1]
      benchDescription = benchSuite.benchmarkDescription()
      pasteToSpreadsheet(args.cred, args.spreadid, data, benchDescription)
      benchSuite.cleanup()