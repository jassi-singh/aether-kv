package engine

import (
	"io"
	"os"
)

type FileInterface interface {
	Append(data []byte) (int64, error)
	ReadAt(offset int64, size uint32) ([]byte, error)
	Close() error
}

type File struct {
	file *os.File
}

func NewFile() (*File, error) {
	file, err := os.OpenFile("/Users/jassi/Playground/aether-kv/data/active.log", os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &File{
		file: file,
	}, nil
}

func (f *File) Append(data []byte) (int64, error) {
	offset, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	_, err = f.file.Write(data)
	if err != nil {
		return 0, err
	}

	err = f.file.Sync()
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (f *File) ReadAt(offset int64, size uint32) ([]byte, error) {
	data := make([]byte, size)
	_, err := f.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (f *File) Close() error {
	return f.file.Close()
}