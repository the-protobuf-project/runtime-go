package evm

// abi.go is the dynamic ABI codec: it moves a record between the store bridge's
// normalized column map and the go-ethereum ABI tuple, by index against the
// resource's columns. The same code serves every resource — no abigen binding.

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/the-protobuf-project/runtime-go/store"
)

// parseABI parses a contract ABI JSON document.
func parseABI(j string) (abi.ABI, error) {
	return abi.JSON(strings.NewReader(j))
}

// inputTuple returns the record tuple type of a write method (create/update).
func inputTuple(parsed abi.ABI, method string) (abi.Type, error) {
	m, ok := parsed.Methods[method]
	if !ok || len(m.Inputs) != 1 || m.Inputs[0].Type.T != abi.TupleTy {
		return abi.Type{}, fmt.Errorf("evm: ABI method %q is not a single-tuple writer", method)
	}
	return m.Inputs[0].Type, nil
}

// outputTuple returns the record tuple type of get()'s single output.
func outputTuple(parsed abi.ABI) (abi.Type, error) {
	m, ok := parsed.Methods["get"]
	if !ok || len(m.Outputs) != 1 || m.Outputs[0].Type.T != abi.TupleTy {
		return abi.Type{}, fmt.Errorf("evm: ABI get() does not return a single tuple")
	}
	return m.Outputs[0].Type, nil
}

// encodeRecord builds the go-ethereum tuple value for a record, taking column
// values by index against the ABI tuple components (which the generator emits in
// resource-column order).
func encodeRecord(tt abi.Type, res *store.Resource, cols map[string]any) (any, error) {
	if len(tt.TupleElems) != len(res.Columns) {
		return nil, fmt.Errorf("evm: ABI tuple has %d fields but resource %q has %d columns", len(tt.TupleElems), res.Name, len(res.Columns))
	}
	out := reflect.New(tt.TupleType).Elem()
	for i, elem := range tt.TupleElems {
		col := res.Columns[i]
		v, err := toABIValue(*elem, col, cols[col.Name])
		if err != nil {
			return nil, fmt.Errorf("evm: encode column %q: %w", col.Name, err)
		}
		out.Field(i).Set(reflect.ValueOf(v))
	}
	return out.Interface(), nil
}

// decodeRecord reflects an ABI tuple value back into a normalized column map.
func decodeRecord(res *store.Resource, tupleVal any) (map[string]any, error) {
	v := reflect.ValueOf(tupleVal)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("evm: expected struct tuple, got %s", v.Kind())
	}
	if v.NumField() != len(res.Columns) {
		return nil, fmt.Errorf("evm: ABI tuple has %d fields but resource %q has %d columns", v.NumField(), res.Name, len(res.Columns))
	}
	out := make(map[string]any, len(res.Columns))
	for i, col := range res.Columns {
		out[col.Name] = fromABIValue(col, v.Field(i).Interface())
	}
	return out, nil
}

// toABIValue converts a normalized bridge value to the Go value go-ethereum's
// ABI codec wants for the component type.
func toABIValue(elem abi.Type, col store.Column, raw any) (any, error) {
	switch elem.T {
	case abi.StringTy:
		return toString(raw), nil
	case abi.BoolTy:
		b, _ := raw.(bool)
		return b, nil
	case abi.BytesTy:
		return toBytes(raw), nil
	case abi.FixedBytesTy:
		arr := reflect.New(elem.GetType()).Elem()
		reflect.Copy(arr, reflect.ValueOf(toBytes(raw)))
		return arr.Interface(), nil
	case abi.IntTy, abi.UintTy:
		n, err := toBig(col, raw)
		if err != nil {
			return nil, err
		}
		return bigToKind(elem.GetType(), n), nil
	default:
		return nil, fmt.Errorf("unsupported ABI type %s", elem.String())
	}
}

// fromABIValue normalizes an ABI Go value back to the bridge's column types.
func fromABIValue(col store.Column, v any) any {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		return x
	case []byte:
		return x
	case *big.Int:
		return intNormalized(col, x.Int64())
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intNormalized(col, rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return intNormalized(col, int64(rv.Uint()))
	case reflect.Array: // fixed bytes
		b := make([]byte, rv.Len())
		reflect.Copy(reflect.ValueOf(b), rv)
		return b
	default:
		return v
	}
}

// intNormalized maps an integer ABI value to the bridge type for its column kind.
func intNormalized(col store.Column, n int64) any {
	switch col.Kind {
	case store.KindTimestamp:
		return time.Unix(n, 0).UTC()
	case store.KindEnum:
		return int32(n)
	default:
		return n
	}
}

// toBig converts a normalized value to a *big.Int for an integer component.
func toBig(col store.Column, raw any) (*big.Int, error) {
	if col.Kind == store.KindTimestamp {
		t, ok := raw.(time.Time)
		if !ok {
			return nil, fmt.Errorf("timestamp column wants time.Time, got %T", raw)
		}
		return big.NewInt(t.Unix()), nil
	}
	switch x := raw.(type) {
	case int64:
		return big.NewInt(x), nil
	case int32:
		return big.NewInt(int64(x)), nil
	case int:
		return big.NewInt(int64(x)), nil
	case uint64:
		return new(big.Int).SetUint64(x), nil
	case float64:
		return big.NewInt(int64(x)), nil
	case *big.Int:
		return x, nil
	case nil:
		return big.NewInt(0), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to integer", raw)
	}
}

// bigToKind renders n as the concrete Go type go-ethereum expects (a sized
// int/uint for ≤64-bit components, *big.Int otherwise).
func bigToKind(rt reflect.Type, n *big.Int) any {
	switch rt.Kind() {
	case reflect.Int8:
		return int8(n.Int64())
	case reflect.Int16:
		return int16(n.Int64())
	case reflect.Int32:
		return int32(n.Int64())
	case reflect.Int64:
		return n.Int64()
	case reflect.Uint8:
		return uint8(n.Uint64())
	case reflect.Uint16:
		return uint16(n.Uint64())
	case reflect.Uint32:
		return uint32(n.Uint64())
	case reflect.Uint64:
		return n.Uint64()
	default:
		return n
	}
}

func toString(raw any) string {
	if s, ok := raw.(string); ok {
		return s
	}
	if raw == nil {
		return ""
	}
	return fmt.Sprint(raw)
}

func toBytes(raw any) []byte {
	switch x := raw.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}
