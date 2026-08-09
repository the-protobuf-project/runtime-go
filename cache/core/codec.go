package core

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// encode turns a caller's value into bytes.
//
// JSON, because a cache is read by whatever is running now and often by
// something else later — another service, a person with redis-cli during an
// incident. A denser encoding would save bytes that a cache, of all stores, can
// afford to spend.
func encode(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cache: cannot encode %T: %w", value, err)
	}
	return body, nil
}

// decode fills a caller's destination.
func decode(body []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("cache: destination is nil")
	}
	if v := reflect.ValueOf(dest); v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("cache: destination must be a non-nil pointer, got %T", dest)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("cache: stored value does not decode into %T: %w", dest, err)
	}
	return nil
}

// sliceTarget validates a list destination and returns the slice to fill and its
// element type, so the strategies that fill one do not each re-derive it.
func sliceTarget(dest any) (reflect.Value, reflect.Type, error) {
	if dest == nil {
		return reflect.Value{}, nil, fmt.Errorf("cache: list destination is nil")
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return reflect.Value{}, nil, fmt.Errorf("cache: list destination must be a non-nil pointer, got %T", dest)
	}
	slice := v.Elem()
	if slice.Kind() != reflect.Slice {
		return reflect.Value{}, nil, fmt.Errorf("cache: list destination must point to a slice, got %T", dest)
	}
	return slice, slice.Type().Elem(), nil
}
