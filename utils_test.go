package recache

import (
	"reflect"
	"testing"
)

// logUnexpected fails the test and prints the values in an
// `expected: X got: Y` format
func logUnexpected(t *testing.T, expected, got interface{}) {
	t.Helper()
	t.Fatalf("\nexpected: %#v\ngot:      %#v", expected, got)
}

// AssertDeepEquals asserts two values are deeply equal or fails the test, if
// not
func assertEquals(t *testing.T, res, std interface{}) {
	t.Helper()
	if !reflect.DeepEqual(res, std) {
		logUnexpected(t, std, res)
	}
}

func decodeJSON(t *testing.T, src *Record, dst interface{}) {
	t.Helper()

	err := src.DecodeJSON(&dst)
	if err != nil {
		t.Fatal(err)
	}
}

func assertJsonStringEquals(t *testing.T, src *Record, std string) {
	t.Helper()

	var res string
	decodeJSON(t, src, &res)
	assertEquals(t, res, std)
}
