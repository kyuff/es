package assert

import (
	"fmt"
	"iter"
	"reflect"
	"regexp"
	"testing"
	"time"
)

type KeyValue[K, V any] struct {
	Key   K
	Value V
}

func EqualSeq2[K, V any](t *testing.T, expected, got iter.Seq2[K, V], equal func(expected, got KeyValue[K, V]) bool) bool {
	var x []KeyValue[K, V]
	var y []KeyValue[K, V]
	for key, val := range expected {
		x = append(x, KeyValue[K, V]{Key: key, Value: val})
	}
	for key, val := range got {
		y = append(y, KeyValue[K, V]{Key: key, Value: val})
	}
	if !Equalf(t, len(x), len(y), "size mismatch") {
		return false
	}

	for i := range len(x) {
		if !equal(x[i], y[i]) {
			return false
		}
	}

	return true
}

func Equal[T comparable](t *testing.T, expected, got T) bool {
	t.Helper()
	return Equalf(t, expected, got, "Items was not equal")
}
func Equalf[T comparable](t *testing.T, expected, got T, format string, args ...any) bool {
	t.Helper()
	if expected != got {
		t.Logf(`
%s
Expected: %v
     Got: %v`, fmt.Sprintf(format, args...), expected, got)
		t.Fail()
		return false
	}
	return true
}

func EqualSlice[T comparable](t *testing.T, expected, got []T) bool {
	t.Helper()
	if len(expected) != len(got) {
		t.Errorf(`Expected %d elements, but got %d`, len(expected), len(got))
		return false
	}

	for i := range len(expected) {
		if !Equal(t, expected[i], got[i]) {
			return false
		}
	}

	return true
}

func EqualSliceFunc[T comparable](t *testing.T, expected, got []T, equal func(want, item T) bool) bool {
	t.Helper()
	if len(expected) != len(got) {
		t.Errorf(`Expected %d elements, but got %d`, len(expected), len(got))
		return false
	}

	for i := range len(expected) {
		if !equal(expected[i], got[i]) {
			return false
		}
	}

	return true
}

func EqualTime(t *testing.T, expected, got time.Time) bool {
	t.Helper()
	if !expected.Equal(got) {
		t.Logf(`
Time was not equal
Expected: %s
     Got: %s`, expected.Format(time.RFC3339), got.Format(time.RFC3339))
		t.Fail()
		return false
	}
	return true
}

func NotEqual[T comparable](t *testing.T, unexpected, got T) bool {
	t.Helper()
	if unexpected == got {
		t.Logf(`
Items was equal
Expected: %v
     Got: %v`, unexpected, got)
		t.Fail()
		return false
	}
	return true
}

func NotNil(t *testing.T, got any) bool {
	t.Helper()
	if reflect.ValueOf(got).IsNil() {
		t.Logf("Expected a value, but got nil")
		t.Fail()
		return false
	}

	return true
}

func Match[T ~string](t *testing.T, expectedRE string, got T) bool {
	t.Helper()
	re, err := regexp.Compile(expectedRE)
	if err != nil {
		t.Fatalf("unexpected regexp: %s", err)
		return false
	}

	match := re.MatchString(string(got))
	if !match {
		t.Logf(`
Must match %q
       Got %q`, expectedRE, got)
		t.Fail()
		return false
	}

	return true
}

func OneOf[T comparable](t *testing.T, items []T, got T) bool {
	t.Helper()
	var found = false
	for _, item := range items {
		if item == got {
			found = true
		}
	}

	if !found {
		t.Logf("Input list: %v", items)
		t.Logf("Did not contain item: %v", got)
		t.Fail()
		return false
	}

	return true
}

func NoneZero[T any, E ~[]T](t *testing.T, got E) bool {
	t.Helper()
	for _, e := range got {
		if reflect.ValueOf(e).IsZero() {
			return false
		}
	}

	return true
}

func NotZero[T any](t *testing.T, got T) bool {
	t.Helper()
	if reflect.ValueOf(got).IsZero() {
		t.Logf("Value %T was zero: %v", got, got)
		t.Fail()
	}
	return true
}

func TimeWithinWindow(t *testing.T, expected time.Time, got time.Time, window time.Duration) bool {
	var (
		from = expected.Add(-1 * window)
		to   = expected.Add(window)
	)

	if got.Before(from) {
		t.Logf("Time was before the window by %s", from.Sub(got))
		t.Fail()
	}

	if got.After(to) {
		t.Logf("Time was after the window by %s", got.Sub(to))
		t.Fail()
	}

	return true
}

func NoError(t *testing.T, got error) bool {
	t.Helper()
	if got != nil {
		t.Logf("Unexpected error: %s", got)
		t.Fail()
		return false
	}

	return true
}

func Error(t *testing.T, got error) bool {
	t.Helper()
	if got == nil {
		t.Logf("Expected error: %s", got)
		t.Fail()
		return false
	}

	return true
}
