package cdc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupChunking(t *testing.T) {
	resp, err := http.Get("http://localhost:8080/data/ids?startAfter=-1&size=100")
	require.NoError(t, err)
	require.Equal(t, resp.StatusCode, 200)
	var ids []int64
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ids))
	require.NoError(t, resp.Body.Close())

	buf := make([]byte, 64*1024)
	var length int
	for _, id := range ids {
		resp, err = http.Get(fmt.Sprintf("http://localhost:8080/data/%d", id))
		require.NoError(t, err)
		require.Equal(t, resp.StatusCode, 200)
		sChunker := NewHashingChunker(NewSingleChunk())
		cChunker := NewHashingChunker(NewCDC(20))
		fChunker := NewHashingChunker(NewFileSpecificChunker(20))
		var sChunks, cChunks, fChunks []LengthHash
		for err == nil {
			length, err = resp.Body.Read(buf)
			sChunks = append(sChunks, sChunker.Next(buf[:length])...)
			cChunks = append(cChunks, cChunker.Next(buf[:length])...)
			fChunks = append(fChunks, fChunker.Next(buf[:length])...)
		}
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, resp.Body.Close())
		sChunks = append(sChunks, sChunker.Flush()...)
		for i, chunk := range sChunks {
			t.Logf("s:%d:%d:%v", id, i+1, &chunk)
		}
		// FIXME cChunks and fChunks are empty...?
		for i, chunk := range cChunks {
			t.Logf("c:%d:%d:%v", id, i+1, &chunk)
		}
		for i, chunk := range fChunks {
			t.Logf("f:%d:%d:%v", id, i+1, &chunk)
		}
	}
}
