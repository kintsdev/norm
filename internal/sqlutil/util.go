package sqlutil

import (
	"fmt"
	"strings"
)

// ConvertQMarksToPgPlaceholders converts '?' placeholders to PostgreSQL-style $1, $2, ...
func ConvertQMarksToPgPlaceholders(s string) string {
	var sb strings.Builder
	index := 1
	for _, r := range s {
		if r == '?' {
			sb.WriteString(fmt.Sprintf("$%d", index))
			index++
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
