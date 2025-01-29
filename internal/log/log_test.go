package log

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	api "proglog/api/v1"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer func(path string) {
				err := os.RemoveAll(path)
				if err != nil {
					t.Logf("failed to remove temp dir: %v", err)
				}
			}(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	recToAppend := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(recToAppend)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	rec, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, recToAppend.Value, rec.Value)
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	read, err := l.Read(1)
	require.Error(t, err)
	require.Nil(t, read)
	var apiErr api.ErrOffsetOutOfRange
	errors.As(err, &apiErr)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, l *Log) {
	recToAppend := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(recToAppend)
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())

	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, l *Log) {
	recToAppend := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(recToAppend)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := l.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := api.Record{}
	err = proto.Unmarshal(b[lenWidth:], &read)
	require.NoError(t, err)
	require.Equal(t, recToAppend.Value, read.Value)
}

func testTruncate(t *testing.T, l *Log) {
	recToAppend := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(recToAppend)
		require.NoError(t, err)
	}
	err := l.Truncate(1)
	require.NoError(t, err)

	_, err = l.Read(0)
	require.Error(t, err)
}
