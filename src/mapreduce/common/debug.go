package common

import (
	"fmt"
)

// Should debugging be enabled?
const debugEnabled = false

// Debug will only print if debugEnabled is true.
func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}
