package internal

// Helper functions to manage parent-child relationships in the children bucket.

// AddChild adds a child relationship between parentID and id.
func AddChild(children Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	return children.Put(key, []byte{})
}

// RemoveChild removes the child relationship between parentID and id.
func RemoveChild(children Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	return children.B().Delete(key)
}
