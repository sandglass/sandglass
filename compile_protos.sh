#!/bin/sh

protoc 	\
		-I/protobuf \
		--gofast_out=plugins=grpc:sgproto \
		--grpc-gateway_out=logtostderr=true:sgproto \
		--swagger_out=logtostderr=true:sgproto/swagger \
		\
		--proto_path=sgproto \
	sgproto/sandglass.proto