package services

import "errors"

var (
	ErrDirNotFound   = errors.New("no such file or directory")
	ErrJsonInput     = errors.New("unexpected end of JSON input")
	ErrInArg         = errors.New("invalid argument")
	ErrKafkaNotFound = errors.New("kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
)
