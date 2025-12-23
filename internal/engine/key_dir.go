package engine

type Key struct {
	FileId    uint32
	ValueSize uint32
	Offset    int64
}

func NewKeyDir() map[string]*Key {
	keyDir := make(map[string]*Key)
	return keyDir
}
