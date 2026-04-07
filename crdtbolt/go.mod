module github.com/tripleclabs/crdt-go/crdtbolt

go 1.25.0

require (
	github.com/tripleclabs/crdt-go v0.0.0
	go.etcd.io/bbolt v1.4.3
	pgregory.net/rapid v1.2.0
)

require golang.org/x/sys v0.29.0 // indirect

replace github.com/tripleclabs/crdt-go => ../
