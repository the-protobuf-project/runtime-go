package opcua

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// floats carries float values JSON cannot: NaN and the infinities. Each float
// is written as the string strconv gives it — "NaN", "+Inf", "1.5" — so the
// payload is still JSON, and reads back into the float type it came from.
var floats floatCodec

// floatCodec is the codec of a value [decode] found a non-finite float in.
type floatCodec struct{}

// Name identifies the codec on a message.
func (floatCodec) Name() string { return "opcua-float" }

// Marshal writes a float or a slice of them, each as its string form.
func (floatCodec) Marshal(v any) ([]byte, error) {
	switch f := v.(type) {
	case float64:
		return json.Marshal(strconv.FormatFloat(f, 'g', -1, 64))
	case float32:
		return json.Marshal(strconv.FormatFloat(float64(f), 'g', -1, 32))
	case []float64:
		return json.Marshal(format(f, 64))
	case []float32:
		return json.Marshal(format(f, 32))
	}
	return nil, fmt.Errorf("opcua: %T is not a float", v)
}

// Unmarshal reads what Marshal wrote into a float, a slice of floats, or an
// any, which receives float64s.
func (floatCodec) Unmarshal(data []byte, dest any) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		switch d := dest.(type) {
		case *float64:
			return parse(one, 64, d)
		case *float32:
			return parse(one, 32, d)
		case *any:
			var f float64
			err := parse(one, 64, &f)
			*d = f
			return err
		}
		return fmt.Errorf("opcua: cannot decode a float into %T", dest)
	}

	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("opcua: not a float payload: %w", err)
	}
	switch d := dest.(type) {
	case *[]float64:
		return parseAll(many, 64, d)
	case *[]float32:
		return parseAll(many, 32, d)
	case *any:
		var fs []float64
		err := parseAll(many, 64, &fs)
		*d = fs
		return err
	}
	return fmt.Errorf("opcua: cannot decode floats into %T", dest)
}

// nonFinite reports whether v is a float, or a slice of them, holding a NaN or
// an infinity.
func nonFinite(v any) bool {
	bad := func(f float64) bool { return math.IsNaN(f) || math.IsInf(f, 0) }
	switch f := v.(type) {
	case float64:
		return bad(f)
	case float32:
		return bad(float64(f))
	case []float64:
		for _, x := range f {
			if bad(x) {
				return true
			}
		}
	case []float32:
		for _, x := range f {
			if bad(float64(x)) {
				return true
			}
		}
	}
	return false
}

// format writes each float in fs at the given bit size.
func format[F float32 | float64](fs []F, bits int) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = strconv.FormatFloat(float64(f), 'g', -1, bits)
	}
	return out
}

// parse reads s at the given bit size into dest.
func parse[F float32 | float64](s string, bits int, dest *F) error {
	f, err := strconv.ParseFloat(s, bits)
	if err != nil {
		return fmt.Errorf("opcua: %q is not a float: %w", s, err)
	}
	*dest = F(f)
	return nil
}

// parseAll reads each of ss at the given bit size into dest.
func parseAll[F float32 | float64](ss []string, bits int, dest *[]F) error {
	out := make([]F, len(ss))
	for i, s := range ss {
		if err := parse(s, bits, &out[i]); err != nil {
			return err
		}
	}
	*dest = out
	return nil
}
