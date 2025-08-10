package core

import (
	"reflect"
	"strings"
	"time"
)

type StructFieldInfo struct {
	Index []int
	Name  string
}

type StructMapping struct {
	FieldsByColumn map[string]StructFieldInfo
	PrimaryColumn  string
	AutoIncrement  bool
	VersionColumn  string
}

func ParseDBTag(tag string) string { return tag }

func StructMapper(t reflect.Type) StructMapping {
	// deref pointer
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	m := StructMapping{FieldsByColumn: make(map[string]StructFieldInfo)}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = ToSnakeCase(f.Name)
		}
		m.FieldsByColumn[strings.ToLower(col)] = StructFieldInfo{Index: f.Index, Name: f.Name}

		// Prefer `norm` tag; fallback to legacy `orm`
		orm := f.Tag.Get("norm")
		if orm == "" {
			orm = f.Tag.Get("orm")
		}
		if orm != "" {
			parts := strings.Split(orm, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "primary_key" {
					m.PrimaryColumn = col
				}
				if p == "auto_increment" {
					m.AutoIncrement = true
				}
				if p == "version" {
					m.VersionColumn = col
				}
			}
		}
		if strings.EqualFold(col, "id") && m.PrimaryColumn == "" {
			m.PrimaryColumn = col
		}
	}
	return m
}

func SetFieldByIndex(v reflect.Value, index []int, value any) {
	// ensure addressable
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	fv := v.FieldByIndex(index)
	if !fv.IsValid() || !fv.CanSet() {
		return
	}
	val := reflect.ValueOf(value)
	if value == nil {
		// set zero if pointer or nullable
		if fv.Kind() == reflect.Ptr {
			fv.Set(reflect.Zero(fv.Type()))
		}
		return
	}
	// Special-case time parsing for TIMESTAMPTZ to time.Time
	if fv.Type() == reflect.TypeOf(time.Time{}) {
		switch t := value.(type) {
		case time.Time:
			fv.Set(reflect.ValueOf(t))
			return
		}
	}
	// try assign with conversion
	if val.Type().AssignableTo(fv.Type()) {
		fv.Set(val)
		return
	}
	if val.Type().ConvertibleTo(fv.Type()) {
		fv.Set(val.Convert(fv.Type()))
		return
	}
	// handle pointer targets
	if fv.Kind() == reflect.Ptr && val.Type().AssignableTo(fv.Type().Elem()) {
		p := reflect.New(fv.Type().Elem())
		p.Elem().Set(val)
		fv.Set(p)
	}
}

func ToSnakeCase(s string) string {
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

// ModelHasSoftDelete checks if a struct has a db:"deleted_at" field
func ModelHasSoftDelete(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = ToSnakeCase(f.Name)
		}
		if strings.EqualFold(col, "deleted_at") {
			return true
		}
	}
	return false
}
