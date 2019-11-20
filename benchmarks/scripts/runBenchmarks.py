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
  parser.add_argument('--cred')
  parser.add_argument('--spreadid')
  return parser.parse_args()

def initBenchmarks(config, proteusTag, ycsbTag, log):
  createOverlayNetwork(log)
  deploy('storage_engine', proteusTag, config, log)
  loadDataset(config, ycsbTag, log)

def getLatestCommitTag(project, branch, log):
  output = runCmd(['cd %s' % (project) + ' && git pull origin %s' % (branch) + ' && git log -1 --pretty=%H'], log)
  output = output.split('\n')
  return output[len(output)-2][0:8]

def createOverlayNetwork(log):
  global STATE
  runCmd(['docker network create -d overlay --attachable proteus_net'], log)
  STATE += 1

def deploy(target, tag, config, log):
  global STATE
  compose_file = config['deployment'][target]
  runCmd(['env PROTEUS_IMAGE_TAG=%s ' % (tag)
    + 'docker stack deploy ' 
    + '--compose-file proteus/deployment/compose-files/%s %s' % (compose_file, target)], log)
  STATE += 1
  checkPlacement(target, config, log)

def loadDataset(config, tag, log):
  table = config['global_config']['table']
  s3_host = config['global_config']['s3_host']
  s3_port = config['global_config']['s3_port']
  s3_access_key_id = config['global_config']['s3_access_key_id']
  workload = config['global_config']['workload']
  record_count = config['global_config']['record_count']
  s3_secret_key = config['global_config']['s3_secret_key']
  runCmd(['docker run --name ycsb --rm --network=proteus_net -e TYPE=load '
    + '-e TABLE=%s ' % (table)
    + '-e S3HOST=%s -e S3PORT=%s ' % (s3_host, s3_port)
    + '-e S3ACCESSKEYID=%s -e S3SECRETKEY=%s ' % (s3_access_key_id, s3_secret_key)
    + '-e WORKLOAD=%s ' % (workload)
    + '-e RECORDCOUNT=%d dvasilas/ycsb:%s' %(record_count, tag) ], log)

def checkPlacement(tier, config, log):
  for service in config['placement']:
    if service.startswith(tier):
      output = ""
      while output.split('\n')[0]  != config['placement'][service]:
      	output = runCmd(['docker service ps --format "{{ .Node }}" %s' % (service)], log)
      if output.split('\n')[0]  != config['placement'][service]:
        print('placement error: '
          + 'service %s should be deployed on node %s, ' % (service, config['placement'][service])
          + 'but is deployed on node %s' % ( output.split('\n')[0]))
        cleanup(log, 0)
        sys.exit()

def cleanup(log, target):
  global STATE
  while STATE > target:
    if STATE == 1:
      runCmd(['docker network rm proteus_net'], log)
    elif STATE == 2:
      runCmd(['docker stack rm storage_engine'], log)
    elif STATE == 3:
      runCmd(['docker stack rm query_engine'], log)
    else:
     print('state should not be %d' % (STATE))
    STATE -= 1

def runOneBenchmark(config, configBench, proteusTag, ycsbTag, outDir, log):
  table = config['global_config']['table']
  s3_host = config['global_config']['s3_host']
  s3_port = config['global_config']['s3_port']
  s3_access_key_id = config['global_config']['s3_access_key_id']
  s3_secret_key = config['global_config']['s3_secret_key']
  proteus_host = config['global_config']['proteus_host']
  proteus_port = config['global_config']['proteus_port']
  workload = config['global_config']['workload']
  record_count = config['global_config']['record_count']
  execution_time= config['global_config']['execution_time']
  warmup_time= config['global_config']['warmup_time']
  query_proportion= configBench['query_proportion']
  update_proportion= configBench['update_proportion']
  cached_query_proportion= configBench['cached_query_proportion']
  threads= configBench['threads']
  output_file= "%dK_%.1f_%.1f_%.1f_%d" % (record_count/1000, cached_query_proportion, query_proportion, update_proportion, threads)

  deploy('query_engine', proteusTag, config, log)
  
  runCmd(['docker run --name ycsb --rm --network=proteus_net -v %s:/ycsb ' % (outDir)
    + '-e TYPE=run -e TABLE=%s ' % (table)
    + '-e S3HOST=%s -e S3PORT=%s ' % (s3_host, s3_port)
    + '-e S3ACCESSKEYID=%s -e S3SECRETKEY=%s ' % (s3_access_key_id, s3_secret_key)
    + '-e PROTEUSHOST=%s -e PROTEUSPORT=%s ' % (proteus_host, proteus_port)
    + '-e WORKLOAD=%s ' % (workload)
    + '-e RECORDCOUNT=%d ' % (record_count)
    + '-e QUERYPROPORTION=%.1f -e UPDATEPROPORTION=%.1f ' % (query_proportion, update_proportion)
    + '-e CACHEDQUERYPROPORTION=%.1f ' % (cached_query_proportion)
    + '-e EXECUTIONTIME=%d -e WARMUPTIME=%d ' % (execution_time, warmup_time)
    + '-e THREADS=%d -e OUTPUT_FILE_NAME=%s ' % (threads, output_file)
    + 'dvasilas/ycsb:%s' % (ycsbTag) ], log)
  cleanup(log, 2)

def runCmd(cmd, log, okToFail=False):
  print(cmd)
  p = subprocess.Popen(cmd, stdout = subprocess.PIPE, universal_newlines=True, shell=True)
  output = ''
  for stdout_line in iter(p.stdout.readline, ""):
    sys.stdout.write(stdout_line)
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
    print('--dest needs an absolute path')
    sys.exit()

  returnCode = runCmd(['mkdir -p %s' % (args.dest)], None)
  if returnCode:
    print('could not create destination directory')
    sys.exit()

  log = open(os.path.join(args.dest, "log"), "w")

  runCmd('docker stack rm query_engine && docker stack rm storage_engine && docker network rm proteus_net', log, True)

  proteusTag = getLatestCommitTag('proteus', 'benchmarks', log)
  ycsbTag = getLatestCommitTag('YCSB', 'proteus', log)

  with open(args.config) as json_file:
    benchmarkConfig = json.load(json_file)
    initBenchmarks(benchmarkConfig, proteusTag, ycsbTag, log)
    data = 'query_proportion, update_proportion, cached_query_proportion, threads, RunTime(ms), [Q]Throughput(ops/sec), [Q]AverageLatency(ms), [Q]MinLatency(ms), [Q]MaxLatency(ms), [Q]95thPercentileLatency(ms), [Q]99thPercentileLatency(ms) \n'
    for benchConfig in benchmarkConfig['benchmarks']:
      record_count = benchmarkConfig['global_config']['record_count']
      query_proportion= benchConfig['query_proportion']
      update_proportion= benchConfig['update_proportion']
      cached_query_proportion= benchConfig['cached_query_proportion']
      threads= benchConfig['threads']
      output_file= '%dK_%.1f_%.1f_%.1f_%d.txt' % (record_count/1000, cached_query_proportion, query_proportion, update_proportion, threads)
      runOneBenchmark(benchmarkConfig, benchConfig, proteusTag, ycsbTag, args.dest, log)
      data += '%.1f, %.1f, %.1f, %d, ' % (query_proportion, update_proportion, cached_query_proportion, threads) 
      data += parseFile(os.path.join(args.dest, output_file)) + '\n'

    data = data[:-1]
    global_config = globalConfigToCsv(benchmarkConfig, proteusTag, ycsbTag)
    pasteToSpreadsheet(args.cred, args.spreadid, data, global_config)

  cleanup(log, 0)
  log.close()
