FROM celrenheit/golang-rocksdb:1.9
LABEL Salim Alami <celrenheit+github@gmail.com>

EXPOSE 8080

WORKDIR $GOPATH/src/github.com/celrenheit/sandglass

COPY . ./

RUN go build -o /usr/bin/sandglass ./cmd/sandglass/main.go && \
    chmod a+x /usr/bin/sandglass

ENTRYPOINT ["/usr/bin/sandglass"]
