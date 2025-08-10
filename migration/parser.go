package migration

import (
	"reflect"
	"strings"
	"time"
)

// fieldTag represents parsed metadata for a struct field
type fieldTag struct {
	Name                string
	DBName              string
	DBType              string
	PrimaryKey          bool
	PKGroup             string
	AutoInc             bool
	Unique              bool
	UniqueGroup         string
	UniqueName          string
	NotNull             bool
	Nullable            bool
	Default             string
	Index               bool
	IndexName           string
	IndexMethod         string // btree, gin, hash
	IndexWhere          string
	OnUpdate            string
	IsPointer           bool
	FKTable             string
	FKColumn            string
	FKName              string
	FKOnDelete          string
	FKOnUpdate          string
	FKDeferrable        bool
	FKInitiallyDeferred bool
	RenameFrom          string
	Collate             string
	Comment             string
}

type modelInfo struct {
	TableName string
	Fields    []fieldTag
}

// splitTagTokens splits a tag string by commas while preserving commas inside parentheses
func splitTagTokens(s string) []string {
	tokens := []string{}
	var b strings.Builder
	depth := 0
	for _, r := range s {
		switch r {
		case '(':
			depth++
			b.WriteRune(r)
		case ')':
			if depth > 0 {
				depth--
			}
			b.WriteRune(r)
		case ',':
			if depth == 0 {
				tok := strings.TrimSpace(b.String())
				if tok != "" {
					tokens = append(tokens, tok)
				}
				b.Reset()
			} else {
				b.WriteRune(r)
			}
		default:
			b.WriteRune(r)
		}
	}
	if t := strings.TrimSpace(b.String()); t != "" {
		tokens = append(tokens, t)
	}
	return tokens
}

// quoteIdent wraps an identifier with double quotes to avoid reserved word collisions
func quoteIdent(id string) string {
	if id == "" {
		return id
	}
	// naive: escape any embedded quotes by doubling
	id = strings.ReplaceAll(id, "\"", "\"\"")
	return "\"" + id + "\""
}

func toSnakeCase(s string) string {
	var out []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			out = append(out, '_', r+('a'-'A'))
		} else {
			if r >= 'A' && r <= 'Z' {
				out = append(out, r+('a'-'A'))
			} else {
				out = append(out, r)
			}
		}
	}
	return string(out)
}

func defaultTableName(t reflect.Type) string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return toSnakeCase(t.Name()) + "s"
}

func parseModel(model any) modelInfo {
	t := reflect.TypeOf(model)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	mi := modelInfo{TableName: defaultTableName(t)}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		db := f.Tag.Get("db")
		if db == "" {
			db = toSnakeCase(f.Name)
		}
		// Prefer `norm` tag; fallback to legacy `orm`
		orm := f.Tag.Get("norm")
		if orm == "" {
			orm = f.Tag.Get("orm")
		}
		ft := fieldTag{Name: f.Name, DBName: db, DBType: mapGoTypeToPgType(f.Type, orm), IsPointer: f.Type.Kind() == reflect.Ptr}
		if orm != "" {
			tokens := splitTagTokens(orm)
			// ignore handling
			ignored := false
			for _, tok := range tokens {
				t := strings.TrimSpace(tok)
				if t == "-" || strings.EqualFold(t, "ignore") {
					ignored = true
					break
				}
			}
			if ignored {
				continue
			}
			for _, p := range tokens {
				p = strings.TrimSpace(p)
				switch {
				case p == "primary_key":
					ft.PrimaryKey = true
				case strings.HasPrefix(strings.ToLower(p), "primary_key:"):
					ft.PrimaryKey = true
					ft.PKGroup = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case p == "auto_increment":
					ft.AutoInc = true
				case p == "unique":
					ft.Unique = true
				case strings.HasPrefix(strings.ToLower(p), "unique:"):
					ft.Unique = true
					ft.UniqueGroup = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "unique_name:"):
					ft.UniqueName = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case p == "not_null":
					ft.NotNull = true
				case strings.EqualFold(p, "nullable"):
					ft.NotNull = false
					ft.Nullable = true
				case strings.HasPrefix(p, "default:"):
					ft.Default = strings.TrimPrefix(p, "default:")
				case p == "index":
					ft.Index = true
				case strings.HasPrefix(strings.ToLower(p), "index:"):
					ft.Index = true
					ft.IndexName = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "index_where:"):
					ft.IndexWhere = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "using:") || strings.HasPrefix(strings.ToLower(p), "index_type:"):
					ft.IndexMethod = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "on_update:"):
					ft.OnUpdate = strings.TrimPrefix(p, "on_update:")
				case p == "version":
					ft.DBType = "BIGINT"
				case strings.HasPrefix(strings.ToLower(p), "fk:") || strings.HasPrefix(strings.ToLower(p), "references:"):
					ref := p[strings.Index(p, ":")+1:]
					if i := strings.Index(ref, "("); i > 0 && strings.HasSuffix(ref, ")") {
						ft.FKTable = ref[:i]
						ft.FKColumn = strings.TrimSuffix(ref[i+1:], ")")
					}
				case strings.HasPrefix(strings.ToLower(p), "fk_name:"):
					ft.FKName = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "on_delete:"):
					ft.FKOnDelete = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "on_update_fk:"):
					ft.FKOnUpdate = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.EqualFold(p, "deferrable"):
					ft.FKDeferrable = true
				case strings.EqualFold(p, "initially_deferred") || strings.EqualFold(p, "initdeferred"):
					ft.FKInitiallyDeferred = true
				case strings.HasPrefix(strings.ToLower(p), "rename:"):
					ft.RenameFrom = strings.TrimPrefix(p, "rename:")
				case strings.HasPrefix(strings.ToLower(p), "collate:"):
					ft.Collate = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "comment:"):
					ft.Comment = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				case strings.HasPrefix(strings.ToLower(p), "type:"):
					ft.DBType = strings.TrimSpace(p[strings.Index(p, ":")+1:])
				default:
					// If token looks like a type override e.g. varchar(50), numeric/decimal, citext
					lp := strings.ToLower(p)
					if strings.Contains(p, "(") || lp == "text" || strings.HasPrefix(lp, "varchar") || strings.HasPrefix(lp, "numeric") || strings.HasPrefix(lp, "decimal") || lp == "citext" {
						ft.DBType = p
					}
				}
			}
		}
		mi.Fields = append(mi.Fields, ft)
	}
	return mi
}

func mapGoTypeToPgType(t reflect.Type, ormTag string) string {
	// strip pointer
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Array:
		// Map fixed-size 16-byte arrays to UUID
		if t.Len() == 16 && t.Elem().Kind() == reflect.Uint8 {
			return "UUID"
		}
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int, reflect.Int64:
		return "BIGINT"
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Uint, reflect.Uint64:
		return "BIGINT"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.String:
		// default string; may be overridden by orm tag
		if strings.Contains(strings.ToLower(ormTag), "varchar") {
			return ormTag
		}
		return "TEXT"
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMPTZ"
		}
		// Heuristic: common UUID struct types from popular packages
		// If the type is named UUID and package path contains "uuid", treat as UUID
		if strings.EqualFold(t.Name(), "UUID") && strings.Contains(strings.ToLower(t.PkgPath()), "uuid") {
			return "UUID"
		}
	}
	return "TEXT"
}
