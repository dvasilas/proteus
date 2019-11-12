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

def listFiles(path):
  files = []
  for r, d, f in os.walk(path):
    for file in f:
      files.append(os.path.join(r, file))
  return files

def parseFile(path):
  with open(path) as fp:
    output = ''
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
  output += overallMeasurements['Throughput(ops/sec)'] + ", "
  output += queryMeasurements['AverageLatency(us)'] + ", "
  output += queryMeasurements['MinLatency(us)'] + ", "
  output += queryMeasurements['MaxLatency(us)'] + ", "
  output += queryMeasurements['95thPercentileLatency(us)'] + ", "
  output += queryMeasurements['99thPercentileLatency(us)']
  return output

def natural_keys(text):
    fileName = text.split("/")[-1].split('.')[0]
    threadId = fileName.split("_")[-1]
    return int(threadId)

def parseMetadata(metadataFile):
  with open(metadataFile) as json_file:
    benchmarkMD = json.load(json_file)
    metadata = ''
    for key in benchmarkMD['benchmarks'][0]:
      metadata += key + ', ' + str(benchmarkMD['benchmarks'][0][key]) + '\n'
    return metadata

def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parse', action='store_true')
  parser.add_argument('--dest')
  parser.add_argument('--config')
  parser.add_argument('--cred')
  parser.add_argument('--spreadid')
  return parser.parse_args()

def initBenchmarks(config, log):
  proteusTag = getLatestCommitTag('proteus', 'benchmarks', log)
  ycsbTag = getLatestCommitTag('YCSB', 'proteus', log)
  print(proteusTag, ycsbTag)
  createOverlayNetwork(log)
  deploy('storage_engine', config, log)
  cleanup(log, 0) 
  loadDataset(config, log)

def getLatestCommitTag(project, branch, log):
  output = runCmd(['cd %s' % (project) + ' && git pull origin %s' % (branch) + ' && git log -1 --pretty=%h'], log)
  output = output.split('\n')
  return output[len(output)-2]

def createOverlayNetwork(log):
  global STATE
  runCmd(['docker network create -d overlay --attachable proteus_net'], log)
  STATE += 1

def deploy(target, config, log):
  global STATE
  compose_file = config['deployment'][target]
  runCmd(['docker stack deploy --compose-file proteus/deployment/compose-files/%s %s' % (compose_file, target)], log)
  STATE += 1
  checkPlacement(target, config, log)

def loadDataset(config, log):
  table = config['global_config']['table']
  s3_host = config['global_config']['s3_host']
  s3_port = config['global_config']['s3_port']
  s3_access_key_id = config['global_config']['s3_access_key_id']
  s3_secret_key = config['global_config']['s3_secret_key'],
  workload = config['global_config']['workload']
  record_count = config['global_config']['record_count']
  runCmd(['docker', 'run', '--name', 'ycsb', '--rm', '--network=proteus_net', '-e', 'TYPE=load', '-e', 'TABLE=%s' % (table),
    '-e', 'S3HOST=%s' % (s3_host), '-e', 'S3PORT=%s' % (s3_port),
    '-e', 'S3ACCESSKEYID=%s' % (s3_access_key_id), '-e', 'S3SECRETKEY=%s' % (s3_secret_key),
    '-e', 'WORKLOAD=%s' % (workload),
    '-e', 'RECORDCOUNT=%d' % (record_count), 'dvasilas/ycsb:proteus'], log)

def checkPlacement(tier, config, log):
  for service in config['placement']:
    if service.startswith(tier):
      output = runCmd(['docker service ps --format "{{ .Node }}" %s' % (service)], log)
      if output.split('\n')[0]  != config['placement'][service]:
        print('placement error: service %s should be deployed on node %s, but is deployed on node %s' 
          % (service, config['placement'][service], output.split('\n')[0].split('"')[1]))
        cleanup(log, 0)

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
  if target == 0:
    print('Cleanup done. Exiting.')
    sys.exit()

def runOneBenchmark(config, configBench, outDir, log):
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

  deploy('query_engine', config, log)

  runCmd(['docker', 'run', '--name', 'ycsb', '--rm', '--network=proteus_net', '-v', '%s:/ycsb' % (outDir),
    '-e', 'TYPE=run', '-e', 'TABLE=%s' % (table),
    '-e', 'S3HOST=%s' % (s3_host), '-e', 'S3PORT=%s' % (s3_port),
    '-e', 'S3ACCESSKEYID=%s' % (s3_access_key_id), '-e', 'S3SECRETKEY=%s' % (s3_secret_key),
    '-e', 'PROTEUSHOST=%s' % (proteus_host), '-e', 'PROTEUSPORT=%s' % (proteus_port), 
    '-e', 'WORKLOAD=%s' % (workload),
    '-e', 'RECORDCOUNT=%d' % (record_count),
    '-e', 'QUERYPROPORTION=%.1f' % (query_proportion), '-e', 'UPDATEPROPORTION=%.1f' % (update_proportion),
    '-e', 'CACHEDQUERYPROPORTION=%.1f' % (cached_query_proportion),
    '-e', 'EXECUTIONTIME=%d' % (execution_time), '-e', 'WARMUPTIME=%d' % (warmup_time),
    '-e', 'THREADS=%d' % (threads), '-e', 'OUTPUT_FILE_NAME=%s' % (output_file),  'dvasilas/ycsb:proteus'], log)
  cleanup(log, 2)

def runCmd(cmd, log):
  print('___', cmd)
  p = subprocess.Popen(cmd, stdout = subprocess.PIPE, universal_newlines=True, shell=True)
  output = ''
  for stdout_line in iter(p.stdout.readline, ""):
    sys.stdout.write(stdout_line)
    if not log == None:
      log.write(stdout_line)
    output += stdout_line
  p.stdout.close()
  returnCode = p.wait()
  if returnCode:
      print("error\n%s\nreturn code: %d" % (cmd, returnCode))
      cleanup(log, 0)
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

  with open(args.config) as json_file:
    benchmarkConfig = json.load(json_file)
    initBenchmarks(benchmarkConfig, log)
    for benchConfig in benchmarkConfig['benchmarks']:
      runOneBenchmark(benchmarkConfig, benchConfig, args.dest, log)
 
  cleanup(log, 0)
  log.close()

    # for benchConfig in benchmarkConfig['benchmarks']:
    #   returnCode = runOneBenchmark(benchmarkConfig, benchConfig, args.dest, log)
    #   if returnCode:
    #     print('error in running a benchmark')
    #     cleanupBenchmarks(log)
    #     sys.exit()

    # cleanupBenchmarks(log)

    # metadata = parseMetadata(args.metadata)

    # bashCommand = "./startBenchmarks"
    # process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    # output, error = process.communicate()
    # print(output)
    # print(error)
    # if error != None:
    #   sys.exit()

    # metadata = parseMetadata(args.metadata)
    # if args.parse:
    #   data = ""
    #   files = listFiles(sys.argv[1])
    #   files.sort(key=natural_keys)
    #   for f in files:
    #     data += parseFile(f) + '\n'
    #     data = data[:-1]
    # else:
    #   with open(args.data, 'r') as f:
    #     data = f.read()
    # pasteToSpreadsheet(args.cred, args.spreadid, data, metadata)

