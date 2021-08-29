package hdfs

import (
	"errors"
	"os"
	"testing"

	"github.com/beyondstorage/go-endpoint"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewClient(t *testing.T) *Storage {
	tmpDir := t.TempDir()

	host := "127.0.0.1"
	port := 9000

	c, err := newStorager(
		ps.WithEndpoint(endpoint.NewTCP(host, port).String()),
		ps.WithWorkDir(tmpDir),
	)
	require.NotNil(t, c)
	require.NoError(t, err)

	return c
}

func TestFormatOsError(t *testing.T) {
	testErr := errors.New("test error")
	tests := []struct {
		name     string
		input    error
		expected error
	}{
		{
			"not found",
			os.ErrNotExist,
			services.ErrObjectNotExist,
		},
		{
			"not supported error",
			testErr,
			services.ErrUnexpected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := formatError(tt.input)
			assert.True(t, errors.Is(err, tt.expected))
		})
	}
}

func TestGetAbsPath(t *testing.T) {
	cases := []struct {
		name         string
		base         string
		path         string
		expectedPath string
	}{
		{"direct path", "", "abc", "abc"},
		{"under direct path", "", "root/abc", "root/abc"},
		{"under direct path", "", "root/abc/", "root/abc"},
		{"under root", "/", "abc", "/abc"},
		{"under exist dir", "/root", "abc", "/root/abc"},
		{"under new dir", "/root", "abc/", "/root/abc"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client := Storage{workDir: tt.base}

			getPath := client.getAbsPath(tt.path)
			assert.Equal(t, tt.expectedPath, getPath)
		})
	}
}
