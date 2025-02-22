package es

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kyuff/es/internal/assert"
)

func VerifyEvents(t *testing.T, events []Content, expected ...any) bool {
	t.Helper()

	if !assert.Equalf(t, len(expected), len(events), "wrong number of events") {
		var sb = &strings.Builder{}
		sb.WriteString("\n Expected events:\n")
		for _, e := range expected {
			sb.WriteString(fmt.Sprintf("  - %s\n", eventName(e)))
		}
		sb.WriteString("\n Got events:\n")
		for _, e := range events {
			sb.WriteString(fmt.Sprintf("  - %s\n", e.EventName()))
		}
		t.Log(sb.String())
		return false
	}

	var valid []bool
	for i := range expected {
		var (
			got      = events[i]
			expected = expected[i]
		)

		if content, ok := expected.(Content); ok {
			valid = append(valid,
				assert.Equalf(t, reflect.TypeOf(content), reflect.TypeOf(got), "event type"),
				assert.Equalf(t, content, got, "event type"),
			)

			continue
		}

		if matcher, ok := expected.(func(event any) bool); ok {
			valid = append(valid,
				assert.Truef(t, matcher(got), "Event %d did not match", i),
			)
		}

	}

	for _, v := range valid {
		if !v {
			t.Fail()
			return false
		}
	}

	return true
}

func eventName(e any) string {
	if named, ok := e.(interface{ Name() string }); ok {
		return named.Name()
	}
	return fmt.Sprintf("%T", e)
}
