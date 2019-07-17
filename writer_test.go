package recache

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestBind(t *testing.T) {
	cache := NewCache(Options{})
	var f *Frontend
	f = cache.NewFrontend(func(k Key, rw *RecordWriter) (err error) {
		switch k.(int) {
		case 0:
			var s Streamer
			s, err = rw.Bind(f, 1)
			if err != nil {
				return
			}
			var data string
			err = s.DecodeJSON(&data)
			if err != nil {
				return
			}
			return json.NewEncoder(rw).Encode(data)
		case 1:
			return json.NewEncoder(rw).Encode("foo")
		default:
			return fmt.Errorf("unknown key: %d", k.(int))
		}
	})

	run := func() {
		s, err := f.Get(0)
		if err != nil {
			t.Fatal(err)
		}
		assertJsonStringEquals(t, s, "foo")
	}

	// Initial population
	run()

	// Read after the data has been populated and made immutable
	run()
}
