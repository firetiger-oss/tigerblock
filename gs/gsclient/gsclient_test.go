package gsclient

import (
	"bytes"
	"net/http"
	"testing"
)

func TestStreamingChunkPool(t *testing.T) {
	c, err := NewGoogleCloudStorageClient(t.Context(), "", WithHTTPClient(http.DefaultClient))
	if err != nil {
		t.Fatal(err)
	}

	f := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buffer, _ := c.streamingChunkBufferPool.Get().(*bytes.Buffer)
			if buffer == nil {
				buffer = bytes.NewBuffer(make([]byte, 0, 10))
			}
			buffer.WriteByte(uint8(i))
			buffer.Reset()
			c.streamingChunkBufferPool.Put(buffer)
		}
	}
	res := testing.Benchmark(f)
	if res.AllocedBytesPerOp() > 0 || res.AllocsPerOp() > 0 {
		t.Errorf("expected zero memory allocations per op, got %s", res.MemString())
	}
}
