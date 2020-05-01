from __future__ import print_function
import argparse
import subprocess as subprocess
import sys
import os
import os.path
import pickle
import json
import time
import requests
from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def runCmd(cmd, log, okToFail=False, showProgress=False):
    if not showProgress:
        print(cmd)
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, universal_newlines=True, shell=True
    )
    output = ""
    for stdout_line in iter(p.stdout.readline, ""):
        if showProgress:
            sys.stdout.write("\r" + " " * 100)
            sys.stdout.flush()
            sys.stdout.write("\r" + stdout_line[:-1])
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
        cleanup(0, log)
        sys.exit()
    return output

def deploy(target, recordCount, systemTag, composeFile, placement, nodeLabels, log):
    global STATE
    cacheSize = recordCount * 5 * 0.2
    runCmd(
        [
            "sudo env PROTEUS_IMAGE_TAG=%s " % (systemTag)
            + "env CACHE_SIZE=%d " % (cacheSize)
            + "docker stack deploy "
            + "--compose-file compose-files/%s %s" % (composeFile, target)
        ],
        log,
    )
    STATE += 1
    checkPlacement(target, placement, nodeLabels, log)


def checkPlacement(target, placement, nodeLabels, log):
    for service in placement:
        if service.startswith(target):
            targetLabel = getattr(placement, service)
            targetNode = getattr(nodeLabels, targetLabel)
            output = ""
            countRetries = 0
            while output.split("\n")[0] != targetNode:
                output = runCmd(
                    ['sudo docker service ps --format "{{ .Node }}" %s' % (service)],
                    log,
                )
                countRetries += 1
                time.sleep(1)
                if countRetries > 10:
                    print(
                        "placement error: "
                        + "service %s should be deployed on node %s, "
                        % (service, targetNode)
                        + "but is deployed on node %s" % (output.split("\n")[0])
                    )
                    cleanup(0, log)
                    sys.exit()


def cleanup(target, log):
    global STATE
    while STATE > target:
        if STATE == 1:
            runCmd(["sudo docker stack rm storage_engine"], log)
        elif STATE == 2:
            runCmd(["sudo docker stack rm query_engine"], log)
        elif STATE == 3:
            print("state should not be %d" % (STATE))
        STATE -= 1


def makeDirLocalRemote(resultDirPath, remoteNodes):
    cmd = "mkdir -p %s " % (resultDirPath)
    for node in remoteNodes:
        cmd += "&& ssh %s 'mkdir -p %s' " % (node, resultDirPath)
    return runCmd([cmd], None)


def loadDataset(benchToolTag, recordCount, dataset, log):
    insertCount = recordCount / dataset.client_num
    insertStart0 = 0
    insertStart1 = insertCount
    insertStart2 = insertStart1 + insertCount
    runCmd(
        [
            "sudo env YCSB_IMAGE_TAG=%s env RECORDCOUNT=%s "
            % (benchToolTag, recordCount)
            + "env INSERTSTART0=%s env INSERTSTART1=%s env INSERTSTART2=%s "
            % (insertStart0, insertStart1, insertStart2)
            + "env INSERTCOUNT=%s " % (insertCount)
            + "docker stack deploy "
            + "--compose-file compose-files/%s load" % (dataset.configFile)
        ],
        log,
    )
    if dataset.client_num == 1:
        waitTermination(["load_client0"], log)
    elif dataset.client_num == 3:
        waitTermination(["load_client0", "load_client1", "load_client2"], log)
    else:
        print("deployment.dataset.client_num can be either 1 or 3")
        cleanup(0, log)
        sys.exit()
    runCmd(["sudo docker stack rm load"], log)


def createResultDir(resultDirPath):
    if makeDirLocalRemote(resultDirPath, ["dc1_node0", "dc2_node0"]):
        print("could not create result output directories")
        sys.exit()


class BenchmarkSuite(dict):
    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def init(
        self, resultDirPath, configFileName, nodeLabelFile,
    ):
        self.resultDirPath = resultDirPath
        self.configFileName = configFileName
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log = open(os.path.join(args.dest, "log_" + timestamp), "w+")
        self.systemTag = getLatestCommitTag(
            self.default_config.system_repo.user,
            self.default_config.system_repo.repo,
            self.default_config.system_repo.branch,
        )
        self.benchToolTag = getLatestCommitTag(
            self.default_config.benchmark_tool_repo.user,
            self.default_config.benchmark_tool_repo.repo,
            self.default_config.benchmark_tool_repo.branch,
        )
        self.nodeLabels = json.loads(
            nodeLabelFile.read(), object_hook=lambda dict: NodeLabels(dict)
        )

    def createBenchmarks(self):
        self.benchmarks = []
        for config in self.benchmark_configs:
            benchmark = Benchmark(self, config)
            self.benchmarks.append(benchmark)

    def cleanupPreviousRun(self):
        runCmd(
            "sudo docker stack rm query_engine "
            + "&& sudo docker stack rm storage_engine "
            + "&& sudo docker stack rm load "
            + "&& sudo docker stack rm run ",
            None,
            True,
        )

    def initSuite(self):
        createOverlayNetwork(self.log)
        deploy(
            "storage_engine",
            self.default_config.record_count,
            self.systemTag,
            getattr(self.deployment, "storage_engine"),
            self.placement,
            self.nodeLabels,
            self.log,
        )
        loadDataset(
            self.benchToolTag,
            self.default_config.record_count,
            self.deployment.dataset,
            self.log,
        )

    def run(self):
        return [bench.run() for bench in self.benchmarks]

    def cleanup(self):
        cleanup(0, self.log)

    def benchmarkDescription(self):
        description = "proteus commit used=%s \n" % (self.systemTag)
        description += "ycsb commit used=%s \n" % (self.benchToolTag)
        description += "execution_time=%d \n" % (self.default_config.execution_time)
        description += "warmup_time=%d \n" % (self.default_config.warmup_time)
        description += "record_count=%d \n" % (self.default_config.record_count)
        description += "storage_engine=%s \n" % (self.deployment.storage_engine)
        description += "query_engine=%s \n" % (self.deployment.query_engine)
        return description


class Benchmark:
    def __init__(self, benchmarkSuite, benchmarkConfig):
        for param in benchmarkSuite.default_config:
            setattr(self, param, benchmarkSuite.default_config[param])
        for param in benchmarkConfig:
            setattr(self, param, benchmarkConfig[param])
        self.resultDirPath = benchmarkSuite.resultDirPath
        self.outputFile = "%dK_%.1f_%.1f_%.1f_%d" % (
            self.record_count / 1000,
            self.cached_query_proportion,
            self.query_proportion,
            self.update_proportion,
            self.threads,
        )
        self.systemTag = benchmarkSuite.systemTag
        self.benchToolTag = benchmarkSuite.benchToolTag
        self.deployment = benchmarkSuite.deployment
        self.placement = benchmarkSuite.placement
        self.nodeLabels = benchmarkSuite.nodeLabels
        self.log = benchmarkSuite.log

    def run(self):
        deploy(
            "query_engine",
            self.record_count,
            self.systemTag,
            getattr(self.deployment, "query_engine"),
            self.placement,
            self.nodeLabels,
            self.log,
        )
        insertCount = self.record_count / len(self.deployment.workload.placement)
        insertStart0 = 0
        insertStart1 = insertCount
        insertStart2 = insertStart1 + insertCount
        runCmd(
            [
                "sudo env YCSB_IMAGE_TAG=%s " % (self.benchToolTag)
                + "env PROTEUSHOST=%s env PROTEUSPORT=%d "
                % (self.proteus_host, self.proteus_port)
                + "env RECORDCOUNT=%s " % (self.record_count)
                + "env INSERTSTART0=%s env INSERTSTART1=%s env INSERTSTART2=%s "
                % (insertStart0, insertStart1, insertStart2)
                + "env INSERTCOUNT=%s " % (insertCount)
                + "env QUERYPROPORTION=%.1f env UPDATEPROPORTION=%.1f "
                % (self.query_proportion, self.update_proportion)
                + "env CACHEDQUERYPROPORTION=%.1f " % (self.cached_query_proportion)
                + "env EXECUTIONTIME=%d env WARMUPTIME=%d "
                % (self.execution_time, self.warmup_time)
                + "env THREADS=%d env OUTPUT_FILE_NAME=%s "
                % (self.threads, self.outputFile)
                + "env OUTDIR=%s " % (self.resultDirPath)
                + "docker stack deploy "
                + "--compose-file compose-files/%s run"
                % (self.deployment.workload.configFile)
            ],
            self.log,
        )
        if len(self.deployment.workload.placement) == 1:
            waitTermination(["run_client0"], self.log)
        elif len(self.deployment.workload.placement) == 3:
            waitTermination(
                ["run_client0", "run_client1", "run_client2"], self.log,
            )
        else:
            print("self.deployment.workload.client_num can be either 1 or 3")
            cleanup(0, self.log)
            sys.exit()
        runCmd(["sudo docker stack rm run"], self.log)
        cleanup(1, self.log)
        for i in self.deployment.workload.placement:
            if i != 0:
                returnCode = runCmd(
                    [
                        "scp dc%s_node0:%s/* %s "
                        % (i, self.resultDirPath, self.resultDirPath)
                    ],
                    self.log,
                )
                if returnCode:
                    print("could not fetch results")
                    sys.exit()
            returnCode = runCmd(
                [
                    "sudo chown -R %s:%s %s"
                    % (os.getgid(), os.getuid(), self.resultDirPath)
                ],
                self.log,
            )
        if len(self.deployment.workload.placement) > 1:
            returnCode = runCmd(
                [
                    "sudo docker run -v %s:/ycsb " % (self.resultDirPath)
                    + "-e MEASUREMENT_RESULTS_DIR=/ycsb "
                    + "-e PREFIX=%s " % (self.outputFile)
                    + "dvasilas/ycsb:%s" % (self.benchToolTag + "_parse")
                ],
                None,
            )
            if returnCode:
                print("could not fetch results")
                sys.exit()

        result = "%.1f, %.1f, %.1f, %d, " % (
            self.query_proportion,
            self.update_proportion,
            self.cached_query_proportion,
            self.threads,
        )
        result += (
            parseFile(os.path.join(self.resultDirPath, self.outputFile + ".txt")) + "\n"
        )
        return result


class NodeLabels(dict):
    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value


if __name__ == "__main__":
    args = parseArgs()

    createResultDir(args.dest)
    with open(args.config) as config_file:
        with open(args.mapping) as label_file:
            benchSuite = json.loads(
                config_file.read(), object_hook=lambda dict: BenchmarkSuite(dict)
            )
            benchSuite.init(
                args.dest, args.config.split(".")[0], label_file,
            )
            benchSuite.cleanupPreviousRun()
            benchSuite.createBenchmarks()
            benchSuite.initSuite()
            results = benchSuite.run()
            data = "query_proportion, update_proportion, cached_query_proportion, threads, [Q]Throughput(ops/sec), [Q]AverageLatency(ms), [Q]MinLatency(ms), [Q]MaxLatency(ms), [Q]95thPercentileLatency(ms), [Q]99thPercentileLatency(ms), [U]Throughput(ops/sec), [U]AverageLatency(ms), [U]MinLatency(ms), [U]MaxLatency(ms), [U]95thPercentileLatency(ms), [U]99thPercentileLatency(ms), [FR]AverageLatency(ms), [FR]MinLatency(ms), [FR]MaxLatency(ms), [FR]95thPercentileLatency(ms), [FR]99thPercentileLatency(ms)\n"
            for res in results:
                data += res
            data = data[:-1]
            benchDescription = benchSuite.benchmarkDescription()
            pasteToSpreadsheet(args.cred, args.spreadid, data, benchDescription)
            benchSuite.cleanup()
