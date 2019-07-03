FROM golang

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends git \
  && mkdir -p $GOPATH/src/github.com/dvasilas \
  && cd $GOPATH/src/github.com/dvasilas \
  && git clone  https://github.com/dvasilas/proteus.git \
  && cd proteus \
  && go get github.com/golang/dep/cmd/dep \
  && dep ensure -v

ADD ./entrypoint.sh /

WORKDIR $GOPATH/src/github.com/dvasilas/proteus

RUN go build -x ./src/qpu/server/server.go

ENTRYPOINT ["/entrypoint.sh"]