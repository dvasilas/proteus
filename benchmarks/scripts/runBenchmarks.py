# usage
# python ycsbToSheet.py path_to_measurement_files path_to_credential_file spreadsheetID sheetID rowIndex columnIndex

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
# 0: clean
# 1: overlay network created
# 2: storage_engine deployed
# 3: query_engine deployed

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
  output += overallMeasurements['RunTime(ms)'] + ", "
  output += queryMeasurements['Throughput(ops/sec)'] + ", "
  output += str(float(queryMeasurements['AverageLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['MinLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['MaxLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['95thPercentileLatency(us)']) / 1000) + ", "
  output += str(float(queryMeasurements['99thPercentileLatency(us)']) / 1000)
  return output

def globalConfigToCsv(config, proteusTag, ycsbTag):
  output = 'proteus commit used=%s \n' %(proteusTag)
  output += 'ycsb commit used=%s \n' %(ycsbTag)
  output += 'execution_time=%d \n' % (config['global_config']['execution_time'])
  output += 'warmup_time=%d \n' % (config['global_config']['warmup_time'])
  output += 'record_count=%d \n' % (config['global_config']['record_count'])
  output += 'storage_engine=%s \n' % (config['deployment']['storage_engine'])
  output += 'query_engine=%s \n' % (config['deployment']['query_engine'])
  return output

def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parse', action='store_true')
  parser.add_argument('--dest')
  parser.add_argument('--config')
  parser.add_argument('--mapping')
  parser.add_argument('--cred')
  parser.add_argument('--spreadid')
  return parser.parse_args()

def initBenchmarks(config, nodeLabelMapping, proteusTag, ycsbTag, log):
  createOverlayNetwork(log)
  deploy('storage_engine', proteusTag, config, nodeLabelMapping, log)
  loadDataset(config, ycsbTag, log)

def getLatestCommitTag(project, branch, log):
  output = runCmd(['cd %s' % (project) + ' && git pull origin %s' % (branch) + ' && git log -1 --pretty=%H'], log)
  output = output.split('\n')
  return output[len(output)-2][0:8]

def createOverlayNetwork(log):
  global STATE
  runCmd(['docker network create -d overlay --attachable proteus_net'], log, True)

def deploy(target, tag, config, nodeLabelMapping, log):
  record_count = config['global_config']['record_count']
  cache_size = record_count * 5 * 0.2
  global STATE
  compose_file = config['deployment'][target]
  runCmd(['env PROTEUS_IMAGE_TAG=%s ' % (tag)
    + 'env CACHE_SIZE=%d ' % (cache_size)
    + 'docker stack deploy '
    + '--compose-file proteus/deployment/compose-files/%s %s' % (compose_file, target)], log)
  STATE += 1
  checkPlacement(target, config, nodeLabelMapping, log)

def loadDataset(config, tag, log):
  dataset = config['deployment']['dataset']
  record_count = config['global_config']['record_count']
  runCmd(['env YCSB_IMAGE_TAG=%s env RECORDCOUNT=%s ' % (tag, record_count)
    + 'docker stack deploy '
    + '--compose-file proteus/deployment/compose-files/%s ycsb_load' % (dataset)
    ], log)
  if '_mb' in dataset:
    waitTermination('ycsb_load_ycsb-0', log)
  elif '_rb' in dataset:
    waitTermination('ycsb_load_ycsb-0', log)
    waitTermination('ycsb_load_ycsb-1', log)
    waitTermination('ycsb_load_ycsb-2', log)
  else:
    print('unknown dataset compose-file name')
    cleanup(log, 0)
    sys.exit()
  runCmd(['docker stack rm ycsb_load'], log)

def waitTermination(service, log):
  complete = False
  while not complete:
    result = runCmd(['docker service ps %s' % (service)
    + " --format '{{.CurrentState}}'"], log, showProgress=True)
    if result.split(' ')[0] == 'Complete':
      complete = True
    else:
      time.sleep(1)


def checkPlacement(tier, config, nodeLabelMapping, log):
  for service in config['placement']:
    if service.startswith(tier):
      targetLabel = config['placement'][service]
      targetNode = nodeLabelMapping[targetLabel]
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
          cleanup(log, 0)
          sys.exit()

def cleanup(log, target):
  global STATE
  while STATE > target:
    if STATE == 1:
      runCmd(['docker stack rm storage_engine'], log)
    elif STATE == 2:
      runCmd(['docker stack rm query_engine'], log)
    elif STATE == 3:
      print('state should not be %d' % (STATE))
    STATE -= 1

def runOneBenchmark(config, configBench, nodeLabelMapping, proteusTag, ycsbTag, outDir, log):
  workload = config['deployment']['workload']
  record_count = config['global_config']['record_count']
  execution_time = config['global_config']['execution_time']
  warmup_time = config['global_config']['warmup_time']
  query_proportion = configBench['query_proportion']
  update_proportion = configBench['update_proportion']
  cached_query_proportion = configBench['cached_query_proportion']
  threads = configBench['threads']
  output_file = "%dK_%.1f_%.1f_%.1f_%d" % (record_count/1000, cached_query_proportion, query_proportion, update_proportion, threads)

  deploy('query_engine', proteusTag, config, nodeLabelMapping, log)

  runCmd(['env YCSB_IMAGE_TAG=%s ' % (ycsbTag)
    + 'env RECORDCOUNT=%s ' % (record_count)
    + 'env QUERYPROPORTION=%.1f env UPDATEPROPORTION=%.1f ' % (query_proportion, update_proportion)
    + 'env CACHEDQUERYPROPORTION=%.1f ' % (cached_query_proportion)
    + 'env EXECUTIONTIME=%d env WARMUPTIME=%d ' % (execution_time, warmup_time)
    + 'env THREADS=%d env OUTPUT_FILE_NAME=%s ' % (threads, output_file)
    + 'env OUTDIR=%s ' % (outDir)
    + 'docker stack deploy '
    + '--compose-file proteus/deployment/compose-files/%s ycsb_run' % (workload)
    ], log)

  if '_mb' in workload:
    waitTermination('ycsb_run_client-0', log)
  elif '_rb' in workload:
    waitTermination('ycsb_run_client-0', log)
    waitTermination('ycsb_run_client-1', log)
    waitTermination('ycsb_run_client-2', log)
  else:
    print('unknown workload compose-file name')
    cleanup(log, 0)
    sys.exit()
  runCmd(['docker stack rm ycsb_run'], log)
  cleanup(log, 1)

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

if __name__ == '__main__':
  args = parseArgs()
  if not os.path.isabs(args.dest):
    resultDirPath = os.path.join(os.path.abspath(os.path.dirname(__file__)), args.dest)
  else:
    resultDirPath = args.dest

  returnCode = runCmd(['mkdir -p %s ' %  (resultDirPath)
    + "&& ssh dc1_node0 'mkdir -p %s' " %  (resultDirPath)
    + "&& ssh dc2_node0 'mkdir -p %s'" %  (resultDirPath)
  ], None)
  if returnCode:
    print('could not create result output directorie')
    sys.exit()

  configFileName = args.config.split(".")[0]
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  log = open(os.path.join(args.dest, 'log_'+configFileName+"_"+timestamp), "w")

  runCmd('docker stack rm query_engine '
    + '&& docker stack rm storage_engine '
    + '&& docker stack rm ycsb_load '
    + '&& docker stack rm ycsb_run ', log, True)

  proteusTag = getLatestCommitTag('proteus', 'benchmarks', log)
  ycsbTag = getLatestCommitTag('YCSB', 'proteus', log)

  with open(args.config) as config_file:
    with open(args.mapping) as label_file:
      benchmarkConfig = json.load(config_file)
      nodeLabelMapping = json.load(label_file)
      # have config object
      # have bench object
      initBenchmarks(benchmarkConfig, nodeLabelMapping, proteusTag, ycsbTag, log)
      data = 'query_proportion, update_proportion, cached_query_proportion, threads, RunTime(ms), [Q]Throughput(ops/sec), [Q]AverageLatency(ms), [Q]MinLatency(ms), [Q]MaxLatency(ms), [Q]95thPercentileLatency(ms), [Q]99thPercentileLatency(ms) \n'
      # make list comprehension: list of bench -> list of results
      for benchConfig in benchmarkConfig['benchmarks']:
        record_count = benchmarkConfig['global_config']['record_count']
        query_proportion= benchConfig['query_proportion']
        update_proportion= benchConfig['update_proportion']
        cached_query_proportion= benchConfig['cached_query_proportion']
        threads = benchConfig['threads']
        runOneBenchmark(benchmarkConfig, benchConfig, nodeLabelMapping, proteusTag, ycsbTag, args.dest, log)
        break

      returnCode = runCmd(['scp dc1_node0:%s/* %s ' %  (resultDirPath, resultDirPath)
        + '&& scp dc2_node0:%s/* %s' %  (resultDirPath, resultDirPath)
      ], None)
      if returnCode:
        print('could not fetch results')
        sys.exit()

      # for benchConfig in benchmarkConfig['benchmarks']:
      #   record_count = benchmarkConfig['global_config']['record_count']
      #   query_proportion= benchConfig['query_proportion']
      #   update_proportion= benchConfig['update_proportion']
      #   cached_query_proportion= benchConfig['cached_query_proportion']
      #   threads = benchConfig['threads']
      #   output_file = '%dK_%.1f_%.1f_%.1f_%d.txt' % (record_count/1000, cached_query_proportion, query_proportion, update_proportion, threads)
      #   data += '%.1f, %.1f, %.1f, %d, ' % (query_proportion, update_proportion, cached_query_proportion, threads)
      #   data += parseFile(os.path.join(args.dest, output_file)) + '\n'

      # data = data[:-1]
      # global_config = globalConfigToCsv(benchmarkConfig, proteusTag, ycsbTag)
      # pasteToSpreadsheet(args.cred, args.spreadid, data, global_config)

  cleanup(log, 0)
  log.close()
