package transform

import (
	"fmt"
	"testing"
)

func Test_evaluate(t *testing.T) {
	var s string
	s = "bigint(20) unsigned"
	fmt.Printf(evaluate(s))
}
