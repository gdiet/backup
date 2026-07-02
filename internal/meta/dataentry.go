package meta

type DataKey struct {
	length int64
	hash   []byte
}

func NewDataKey(length int64, hash []byte) DataKey { return DataKey{length, hash} }
func (d DataKey) Length() int64                    { return d.length }
func (d DataKey) Hash() []byte                     { return d.hash }

func (d DataKey) toBytes() []byte {
	result := make([]byte, len(d.hash)+8)
	i64w(result, d.length)
	copy(result[8:], d.hash)
	return result
}

type DataArea struct {
	start int64
	end   int64
}

func NewDataArea(start int64, end int64) DataArea { return DataArea{start, end} }
func (d DataArea) Start() int64                   { return d.start }
func (d DataArea) End() int64                     { return d.end }

func (d DataArea) toBytes() []byte {
	result := make([]byte, 16)
	i64w(result[:8], d.start)
	i64w(result[8:16], d.end)
	return result
}

func dataAreaFrom(b []byte) DataArea {
	return DataArea{b64i(b[:8]), b64i(b[8:16])}
}
