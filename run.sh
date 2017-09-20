#!/bin/sh


go run cmd/sandglass/main.go --name ${NAME} \
        --discovery-backend etcd --discovery-addrs etcd:4001 \
        --advertise-addr ${ADVERTISE_ADDR} --http-addr ${HTTP_ADDR} \
        --grpc-addr ${GRPC_ADDR} \
        --dbpath /tmp/${NAME}db \
        --initial-peers ${INITIAL_PEERS}