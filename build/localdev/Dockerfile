FROM golang

ENV GO111MODULE=on

WORKDIR /

COPY go.mod .
COPY go.sum .

RUN go mod download

WORKDIR /app/proteus/
COPY . .

COPY ./configs /etc

RUN make build

ENTRYPOINT ["/app/proteus/bin/qpu"]