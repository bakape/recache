package benchmarks

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Append a random buffer of the specified `size` to `in` and sleep for `dur`
// amount of time to simulate the time cost of retrieving a resource
func appendBuffer(in []byte, size int, dur time.Duration) (
	out []byte, err error,
) {
	if size == 0 {
		return nil, nil
	}

	raw := make([]byte, size/2)
	_, err = rand.Read(raw)
	if err != nil {
		return
	}

	if cap(in)-len(in) < size {
		out = make([]byte, len(in)+size)
		copy(out, in)
	} else {
		out = in[:len(in)+size]
	}
	hex.Encode(out[len(in):], raw)

	time.Sleep(dur)
	return
}

// Concatenate input buffers and append `size` amount of random data and sleep
// for `dur` amount of time to emulate retrieving the data
func extend(size int, dur time.Duration, in ...[]byte) (out []byte, err error) {
	cap := 0
	for _, b := range in {
		cap += len(b)
	}
	out = make([]byte, 0, cap+size)
	for _, b := range in {
		out = append(out, b...)
	}

	out, err = appendBuffer(out, size, dur)
	return
}

// Generate a fresh mock configuration for use by the entire site
func generateConfiguration() (out []byte, err error) {
	return appendBuffer(nil, 1<<10, time.Millisecond*30)
}

// Generate fresh content to be rendered into the main part of the site
func generateContent(config []byte) (out []byte, err error) {
	return appendBuffer(config, 3<<10, time.Millisecond*50)
}

// Generate a fresh mock header portion of the sample page from the
// configuration
func generateHeader(config []byte) (out []byte, err error) {
	return extend(1<<10, time.Millisecond, config)
}

// Generate a fresh mock content portion of the sample page from raw content
// data and configuration
func generatePageMiddle(content, configuration []byte) (
	out []byte, err error,
) {
	return extend(4<<10, time.Millisecond, content, configuration)
}

// Generate a fresh mock footer portion of the sample page from the
// configuration
func generateFooter(config []byte) (out []byte, err error) {
	return extend(1<<10, time.Millisecond, config)
}

// Generate a fresh version of the entire page
func generatePage() (out []byte, err error) {
	config, err := generateConfiguration()
	if err != nil {
		return
	}
	content, err := generateContent(config)
	if err != nil {
		return
	}

	header, err := generateHeader(config)
	if err != nil {
		return
	}
	middle, err := generatePageMiddle(content, config)
	if err != nil {
		return
	}
	footer, err := generateFooter(config)
	if err != nil {
		return
	}
	return bytes.Join([][]byte{header, middle, footer}, nil), nil
}
