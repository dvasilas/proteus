#!/usr/local/bin/python

# Based on
# https://gist.github.com/obiyann/b8cf6a233b3523a492c3e5db79ad6019
# https://gist.github.com/obiyann/6d48e5028ec2f76e4067e0fecca9fc83
#
# Use as
# python tags.py --endpointport 8000 --bucket local-s3
#
# To run TagBox
# docker run -p 8080:8080 -e "MB_KEY=$MB_KEY" machinebox/tagbox
#
# requires
# MB_KEY env variable
import sys, getopt
import argparse
import os
import shutil
import ntpath
import json
from pprint import pprint
import subprocess
import shlex
from os import listdir
from os.path import isfile, join

def main(argv):
	filepath = ""

	# Parse arguments. File full path and op (upload/download)
	parser = argparse.ArgumentParser()
	parser.add_argument('--endpointport', help='')
	parser.add_argument('--bucket', help='')
	parser.add_argument('--dir', help='')
	args = vars(parser.parse_args())

	zenkoendpoint = args['endpointport']
	zenkobucket = args['bucket']
	path = args['dir']

	images = [f for f in listdir(path) if isfile(join(path, f))]

	for filename in images:
		filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), path, filename)

		print(filepath)
		print(filename)

		# Upload file with public read access
		print("Uploading file to Zenko ...")
		command = 's3cmd put --host=127.0.0.1:' + zenkoendpoint + ' --acl-public ' + filepath + ' s3://'+ zenkobucket + '/' + filename
		print(command)
		os.system(command)

		# Analyze file with machinebox
		print("MachineBox analyzing file ...")
		command = 'curl -H \'Content-Type: application/json\' -d \'{"url":"http://host.docker.internal:' + zenkoendpoint  + '/' + zenkobucket + '/' + filename + '"}\' http://localhost:8080/tagbox/check'
		print(command)
		result = subprocess.check_output(command, shell=True)
		print(result)

		json_res = json.loads(result.decode('utf-8'))

		print(json_res["tags"])

		tagset_str = ''

		if len(json_res["tags"]) != 0:
			i=0
			while i<len(json_res["tags"]):
				json_res["tags"][i]["tag"] = json_res["tags"][i]["tag"].replace(" ","-")
				tagset_str = tagset_str + ' --add-header=x-amz-meta-f-'+str(json_res["tags"][i]["tag"]) + ':' + str(json_res["tags"][i]["confidence"])
				i=i+1

		command = 's3cmd modify --host=127.0.0.1:' + zenkoendpoint + ' s3://'+ zenkobucket + '/' + filename + ' ' + tagset_str
		print(command)
		os.system(command)

if __name__ == "__main__":
    main(sys.argv[1:])
