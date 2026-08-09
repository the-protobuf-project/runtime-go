package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	goredis "github.com/redis/go-redis/v9"
)

// setNX claims a key only if it is absent, reporting whether this caller won.
//
// go-redis deprecated its own SetNX in favor of SetArgs with Mode "NX"; the
// behavior is the same, and a nil reply means someone else already holds the
// key. Content reservation depends on this being atomic — two writers racing on
// identical content must not both win.
func setNX(ctx context.Context, rdb goredis.UniversalClient, key, value string) (bool, error) {
	err := rdb.SetArgs(ctx, key, value, goredis.SetArgs{Mode: "NX"}).Err()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// sliceTarget validates a List destination and returns the slice to fill along
// with its element type, so List can decode into whatever concrete type the
// caller asked for.
func sliceTarget(dest any) (reflect.Value, reflect.Type, error) {
	if dest == nil {
		return reflect.Value{}, nil, fmt.Errorf("database/redis: list destination is nil")
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return reflect.Value{}, nil, fmt.Errorf("database/redis: list destination must be a non-nil pointer, got %T", dest)
	}
	slice := v.Elem()
	if slice.Kind() != reflect.Slice {
		return reflect.Value{}, nil, fmt.Errorf("database/redis: list destination must point to a slice, got %T", dest)
	}
	return slice, slice.Type().Elem(), nil
}
