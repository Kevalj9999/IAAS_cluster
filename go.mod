module github.com/Kevalj9999/IAAS-cluster

go 1.25.1

require github.com/hashicorp/raft v1.3.11 // optional: used later for consensus (example)

require (
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/hashicorp/go-hclog v1.6.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
)

require (
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	golang.org/x/sys v0.0.0-20220503163025-988cb79eb6c6 // indirect
)
