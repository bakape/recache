package recache

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/onsi/gomega"
)

// AssertJSON equality to unencoded data
func AssertJSON(t *testing.T, r io.Reader, std interface{}) {
	t.Helper()

	res, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("json: %s", string(res))

	// Strip trailing newline - encoder artefact
	if l := len(res); l != 0 && res[l-1] == '\n' {
		res = res[:l-1]
	}

	stdJSON, err := json.Marshal(std)
	if err != nil {
		t.Fatal(err)
	}

	gomega.NewGomegaWithT(t).Expect(res).To(gomega.MatchJSON(stdJSON))
}

// LogUnexpected fails the test and prints the values in an
// `expected: X got: Y` format
func LogUnexpected(t *testing.T, expected, got interface{}) {
	t.Helper()
	t.Fatalf("\nexpected: %#v\ngot:      %#v", expected, got)
}

// AssertDeepEquals asserts two values are deeply equal or fails the test, if
// not
func AssertEquals(t *testing.T, res, std interface{}) {
	t.Helper()
	if !reflect.DeepEqual(res, std) {
		LogUnexpected(t, std, res)
	}
}

func unzip(t *testing.T, src io.Reader) string {
	t.Helper()

	r, err := gzip.NewReader(src)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Remove training newline
	if l := len(buf); l != 0 && buf[l-1] == '\n' {
		buf = buf[:l-1]
	}

	return string(buf)
}
