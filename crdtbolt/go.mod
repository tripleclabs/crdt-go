module github.com/3clabs/crdt/crdtbolt

go 1.25.0

require (
	github.com/3clabs/crdt v0.0.0
	go.etcd.io/bbolt v1.4.3
	pgregory.net/rapid v1.2.0
)

require golang.org/x/sys v0.29.0 // indirect

replace github.com/3clabs/crdt => ../
