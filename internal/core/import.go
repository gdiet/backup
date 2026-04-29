package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gdiet/backup/internal/cdc"
	"lukechampine.com/blake3"
)

/*
CREATE TABLE data (
  alg  VARCHAR(1),
  id   BIGINT,
  part INT,
  len  BIGINT,
  hash VARCHAR(16)
);

INSERT INTO data (alg, id, part, len, hash) VALUES ('s', 470, 1, 1770848, '893af516119bb335');
*/

// Import gets the repository path as parameter
func Import(_ string) error {
	startAfter := int64(-1)
	size := 0
	count := 0
	for {
		ids, err := getIDs(startAfter)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}

		buf := make([]byte, 64*1024)
		var length int
		for _, id := range ids {
			add, err := getData(id, length, buf)
			if err != nil {
				return err
			}
			size += add
			count++
			if count%20 == 0 {
				_, _ = fmt.Fprintf(os.Stderr, "%d - %d\r", count, size)
			}
		}
		startAfter = ids[len(ids)-1]
	}
	return nil
}

func getIDs(startAfter int64) ([]int64, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8080/data/ids?startAfter=%d&size=100", startAfter))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /data/ids: unexpected status: %s", resp.Status)
	}
	var ids []int64
	err = json.NewDecoder(resp.Body).Decode(&ids)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func getData(id int64, length int, buf []byte) (int, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8080/data/%d", id))
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET /data/%d - unexpected status: %s", id, resp.Status)
	}
	cdcConfig20, _ := cdc.NewConfig(20)
	cdcConfig19, _ := cdc.NewConfig(19)
	cdcChunker20 := cdcConfig20.NewCDC()
	cdcChunker19 := cdcConfig19.NewCDC()
	fsChunker := cdcConfig20.NewFileSpecificChunker()
	sChunker := cdc.NewHashingChunker(blake3.New(20, nil), cdc.NewSingleChunk())
	c20Chunker := cdc.NewHashingChunker(blake3.New(20, nil), cdcChunker20)
	c19Chunker := cdc.NewHashingChunker(blake3.New(20, nil), cdcChunker19)
	fChunker := cdc.NewHashingChunker(blake3.New(20, nil), fsChunker)
	var sChunks, cChunks20, cChunks19, fChunks []cdc.LengthHash
	for err == nil {
		length, err = resp.Body.Read(buf)
		sChunks = append(sChunks, sChunker.Next(buf[:length])...)
		cChunks20 = append(cChunks20, c20Chunker.Next(buf[:length])...)
		cChunks19 = append(cChunks19, c19Chunker.Next(buf[:length])...)
		fChunks = append(fChunks, fChunker.Next(buf[:length])...)
	}
	if err != io.EOF {
		return 0, fmt.Errorf("GET /data/%d - expected EOF: %s", id, resp.Status)
	}
	err = resp.Body.Close()
	if err != nil {
		return 0, err
	}
	sChunks = append(sChunks, sChunker.Flush()...)
	cChunks20 = append(cChunks20, c20Chunker.Flush()...)
	cChunks19 = append(cChunks19, c19Chunker.Flush()...)
	fChunks = append(fChunks, fChunker.Flush()...)
	for i, chunk := range sChunks {
		fmt.Printf("s00,%d,%d,%v\n", id, i+1, &chunk)
	}
	for i, chunk := range cChunks20 {
		fmt.Printf("c20,%d,%d,%v\n", id, i+1, &chunk)
	}
	for i, chunk := range cChunks19 {
		fmt.Printf("c19,%d,%d,%v\n", id, i+1, &chunk)
	}
	for i, chunk := range fChunks {
		fmt.Printf("f20,%d,%d,%v\n", id, i+1, &chunk)
	}
	return sChunks[0].Length, nil
}
