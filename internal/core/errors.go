package core

import "errors"

var (
	ErrRPCTimeout    = errors.New("rpc timeout")
	ErrRPCRateLimit  = errors.New("rpc rate limited")
	ErrBlockNotFound = errors.New("block not found")
	ErrParseEvent    = errors.New("event parse failed")
)
