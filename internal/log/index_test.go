package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			t.Logf("failed to remove temporary file: %v", err)
		}
	}(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, entry := range entries {
		err = idx.Write(entry.Off, entry.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(entry.Off))
		require.NoError(t, err)
		require.Equal(t, entry.Pos, pos)
	}

	// index and scanner should error when reading past last entry
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the previously existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
