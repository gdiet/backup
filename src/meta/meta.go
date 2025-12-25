package meta

import "go.etcd.io/bbolt"

type Metadata struct {
	db           *bbolt.DB
	treeKey      []byte
	childrenKey  []byte
	dataKey      []byte
	freeAreasKey []byte
}
