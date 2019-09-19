from concurrent import futures
import time
import logging
import argparse
import grpc
import s3client_pb2
import s3client_pb2_grpc
import boto3
from botocore.exceptions import ClientError

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class S3ClientServer(s3client_pb2_grpc.S3Servicer):
    def __init__(self, s3Endpoint):
        self.s3 = boto3.client('s3', aws_access_key_id='accessKey1', aws_secret_access_key='verySecretKey1', endpoint_url=s3Endpoint)

    def create_bucket(self, bucket_name):
      self.s3.create_bucket(Bucket=bucket_name)

    def CreateBucket(self, request, context):
      try:
        self.create_bucket(request.bucket_name)
      except ClientError as e:
          logging.error(e)
          return s3client_pb2.Reply(reply=-1)
      return s3client_pb2.Reply(reply=0)

    def PutObject(self, request_iterator, context):
        for request in request_iterator:
            md = {}
            for k in request.x_amz_meta:
                md[k] = request.x_amz_meta[k]
            try:
                self.s3.put_object(Bucket=request.bucket_name, Key=request.object_name, Body=request.content.encode(), Metadata=md)
            except ClientError as e:
                logging.error(e)
                yield s3client_pb2.Reply(reply=-1)
            yield s3client_pb2.Reply(reply=0)

    def UpdateTags(self, request_iterator, context):
      for request in request_iterator:
        try:
            res = self.s3.head_object(Bucket=request.bucket_name, Key=request.object_name)
            md = res['Metadata']
            for k in request.x_amz_meta:
              md[k] = request.x_amz_meta[k]
            self.s3.copy_object(Bucket=request.bucket_name, Key=request.object_name, CopySource={'Bucket': request.bucket_name, 'Key': request.object_name}, MetadataDirective='REPLACE',  Metadata=md)
        except ClientError as e:
            logging.error(e)
            yield s3client_pb2.Reply(reply=-1)
        yield s3client_pb2.Reply(reply=0)

def serve(port, s3):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    s3client_pb2_grpc.add_S3Servicer_to_server(S3ClientServer(s3), server)
    server.add_insecure_port('[::]:'+str(port))
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='the port that the server listens to', type=int)
    parser.add_argument('--s3', help='s3 endpont', type=str)
    args = parser.parse_args()
    print(args)
    logging.basicConfig()
    serve(args.port, args.s3)
