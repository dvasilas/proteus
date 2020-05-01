FROM golang

RUN set -x \
  && apt-get update
  # && apt-get install -y --no-install-recommends python3 python3-pip \
  # && pip3 install boto3 grpcio grpcio-tools

WORKDIR $GOPATH/src/github.com/dvasilas/proteus

COPY . .