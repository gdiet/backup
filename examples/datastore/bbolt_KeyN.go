package main

import (
	"fmt"
	"log"
	"os"

	"go.etcd.io/bbolt"
)

// This example demonstrates how the KeyN statistic of a bbolt bucket changes
// as entries are added to the bucket. They do NOT reflect uncommitted changes
// within a transaction.
func main() {
	var file *os.File
	var err error
	var db *bbolt.DB
	var bucket *bbolt.Bucket
	bucketKey := []byte("bucket")

	if file, err = os.CreateTemp("", "test-*.db"); err != nil {
		log.Printf("Failed to create temp db file: %v", err)
	}
	if err = file.Close(); err != nil {
		log.Printf("Failed to close temp file: %v", err)
	}

	if db, err = bbolt.Open(file.Name(), 0600, nil); err != nil {
		os.Remove(file.Name())
		log.Printf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(file.Name())
	}()

	err = db.Update(func(tx *bbolt.Tx) error {
		if bucket, err = tx.CreateBucket(bucketKey); err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		log.Printf("Update #1: KeyN = %d", bucket.Stats().KeyN)
		bucket.Put([]byte("key"), []byte("value"))
		log.Printf("Update #2: KeyN = %d", bucket.Stats().KeyN)
		return nil
	})
	if err != nil {
		log.Printf("DB update failed: %v", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		if bucket = tx.Bucket(bucketKey); bucket == nil {
			return fmt.Errorf("failed to open bucket: %w", err)
		}
		log.Printf("Update #3: KeyN = %d", bucket.Stats().KeyN)
		bucket.Put([]byte("key2"), []byte("value2"))
		log.Printf("Update #4: KeyN = %d", bucket.Stats().KeyN)
		return nil
	})
	if err != nil {
		log.Printf("DB update failed: %v", err)
	}
}
