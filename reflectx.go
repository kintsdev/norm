package kintsnorm

import (
	"reflect"
	"strings"
	"time"
)

type structFieldInfo struct {
	index []int
	name  string
}

type structMapping struct {
	fieldsByColumn map[string]structFieldInfo
	primaryColumn  string
	autoIncrement  bool
	versionColumn  string
}

func parseDBTag(tag string) string { return tag }

func structMapper(t reflect.Type) structMapping {
	// deref pointer
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	m := structMapping{fieldsByColumn: make(map[string]structFieldInfo)}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = toSnakeCase(f.Name)
		}
		m.fieldsByColumn[strings.ToLower(col)] = structFieldInfo{index: f.Index, name: f.Name}

		orm := f.Tag.Get("orm")
		if orm != "" {
			parts := strings.Split(orm, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "primary_key" {
					m.primaryColumn = col
				}
				if p == "auto_increment" {
					m.autoIncrement = true
				}
				if p == "version" {
					m.versionColumn = col
				}
			}
		}
		if strings.EqualFold(col, "id") && m.primaryColumn == "" {
			m.primaryColumn = col
		}
	}
	return m
}

func setFieldByIndex(v reflect.Value, index []int, value any) {
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

// modelHasSoftDelete checks if a struct has a db:"deleted_at" field
func modelHasSoftDelete(t reflect.Type) bool {
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
			col = toSnakeCase(f.Name)
		}
		if strings.EqualFold(col, "deleted_at") {
			return true
		}
	}
	return false
}
