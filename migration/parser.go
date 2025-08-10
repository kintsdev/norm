package migration

import (
	"reflect"
	"strings"
	"time"
)

// fieldTag represents parsed metadata for a struct field
type fieldTag struct {
	Name       string
	DBName     string
	DBType     string
	PrimaryKey bool
	AutoInc    bool
	Unique     bool
	NotNull    bool
	Default    string
	Index      bool
	OnUpdate   string
	IsPointer  bool
	FKTable    string
	FKColumn   string
	RenameFrom string
}

type modelInfo struct {
	TableName string
	Fields    []fieldTag
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
		orm := f.Tag.Get("orm")
		ft := fieldTag{Name: f.Name, DBName: db, DBType: mapGoTypeToPgType(f.Type, orm), IsPointer: f.Type.Kind() == reflect.Ptr}
		if orm != "" {
			for _, p := range strings.Split(orm, ",") {
				p = strings.TrimSpace(p)
				switch {
				case p == "primary_key":
					ft.PrimaryKey = true
				case p == "auto_increment":
					ft.AutoInc = true
				case p == "unique":
					ft.Unique = true
				case p == "not_null":
					ft.NotNull = true
				case strings.HasPrefix(p, "default:"):
					ft.Default = strings.TrimPrefix(p, "default:")
				case p == "index":
					ft.Index = true
				case strings.HasPrefix(p, "on_update:"):
					ft.OnUpdate = strings.TrimPrefix(p, "on_update:")
				case p == "version":
					ft.DBType = "BIGINT"
				case strings.HasPrefix(strings.ToLower(p), "fk:") || strings.HasPrefix(strings.ToLower(p), "references:"):
					ref := p[strings.Index(p, ":")+1:]
					if i := strings.Index(ref, "("); i > 0 && strings.HasSuffix(ref, ")") {
						ft.FKTable = ref[:i]
						ft.FKColumn = strings.TrimSuffix(ref[i+1:], ")")
					}
				case strings.HasPrefix(strings.ToLower(p), "rename:"):
					ft.RenameFrom = strings.TrimPrefix(p, "rename:")
				default:
					// If token looks like a type override e.g. varchar(50)
					if strings.Contains(p, "(") || strings.EqualFold(p, "text") || strings.HasPrefix(strings.ToLower(p), "varchar") {
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
	}
	return "TEXT"
}
