package sqlutil

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// ConvertQMarksToPgPlaceholders converts '?' placeholders to PostgreSQL-style $1, $2, ...
func ConvertQMarksToPgPlaceholders(s string) string {
	var sb strings.Builder
	sb.Grow(len(s) + 8) // small headroom
	index := 1
	for i := 0; i < len(s); i++ {
		if s[i] == '?' {
			sb.WriteByte('$')
			// append decimal digits of index without fmt allocations
			buf := strconv.AppendInt(nil, int64(index), 10)
			sb.Write(buf)
			index++
			continue
		}
		sb.WriteByte(s[i])
	}
	return sb.String()
}

// ConvertNamedToPgPlaceholders converts :name placeholders into $1, $2, ... and returns ordered args.
// Rules:
// - Named identifiers must match [A-Za-z_][A-Za-z0-9_]*
// - Occurrences inside single-quoted string literals are ignored
// - For slice/array values, expands to multiple placeholders separated by ", "
// - Repeated scalar names reuse the same placeholder index
// - Repeated slice names are not supported and will error to avoid ambiguous expansion
func ConvertNamedToPgPlaceholders(sql string, named map[string]any) (string, []any, error) {
	var out strings.Builder
	args := make([]any, 0, len(named))
	nameToIndex := map[string]int{}
	inSingle := false
	argIndex := 1
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' { // toggle on single quotes
			inSingle = !inSingle
			out.WriteByte(ch)
			continue
		}
		if inSingle {
			out.WriteByte(ch)
			continue
		}
		if ch == ':' {
			// Handle Postgres cast '::'
			if i+1 < len(sql) && sql[i+1] == ':' {
				out.WriteString("::")
				i++ // skip the second ':'
				continue
			}
			// Parse identifier
			if i+1 >= len(sql) {
				out.WriteByte(ch)
				continue
			}
			start := i + 1
			if !isIdentStart(sql[start]) {
				out.WriteByte(ch)
				continue
			}
			j := start + 1
			for j < len(sql) && isIdentPart(sql[j]) {
				j++
			}
			name := sql[start:j]
			val, ok := named[name]
			if !ok {
				return "", nil, fmt.Errorf("missing named param: %s", name)
			}
			// If slice/array expand
			if isSliceButNotBytes(val) {
				if _, seen := nameToIndex[name]; seen {
					return "", nil, fmt.Errorf("repeated slice named param not supported: %s", name)
				}
				rv := reflect.ValueOf(val)
				ln := rv.Len()
				if ln == 0 {
					// Produce an always-false predicate "(select 1 where false)" style; simplest: write 'NULL'
					// but keep SQL valid: use '(NULL)'
					out.WriteString("(NULL)")
				} else {
					out.WriteByte('(')
					for k := 0; k < ln; k++ {
						if k > 0 {
							out.WriteString(", ")
						}
						out.WriteString(fmt.Sprintf("$%d", argIndex))
						args = append(args, rv.Index(k).Interface())
						argIndex++
					}
					out.WriteByte(')')
				}
				nameToIndex[name] = -1 // mark as expanded
			} else {
				if idx, seen := nameToIndex[name]; seen && idx > 0 {
					out.WriteString(fmt.Sprintf("$%d", idx))
				} else {
					out.WriteString(fmt.Sprintf("$%d", argIndex))
					args = append(args, val)
					nameToIndex[name] = argIndex
					argIndex++
				}
			}
			i = j - 1
			continue
		}
		out.WriteByte(ch)
	}
	return out.String(), args, nil
}

func isIdentStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}

func isIdentPart(b byte) bool {
	return isIdentStart(b) || (b >= '0' && b <= '9')
}

func isSliceButNotBytes(v any) bool {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// exclude []byte
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			return false
		}
		return true
	}
	return false
}
