FROM golang

WORKDIR $GOPATH/src/github.com/dvasilas/proteus

COPY . .
