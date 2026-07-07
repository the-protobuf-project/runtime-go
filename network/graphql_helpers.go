// Helpers for building GraphQL variables and dynamic operations.
package network

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// StructToMap converts a struct with json tags into a map[string]interface{} via
// JSON marshal/unmarshal. Useful for building GraphQL variables from structs.
func StructToMap(input interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// BuildGraphQLArgs formats a map of variables as a GraphQL arguments string
// (e.g. `id: "1", name: "foo"`) for use in inline mutation/query strings.
func BuildGraphQLArgs(variables map[string]interface{}) string {
	args := make([]string, 0, len(variables))
	for key, value := range variables {
		var argValue string
		switch v := value.(type) {
		case string:
			argValue = fmt.Sprintf("%s: %q", key, v)
		case int, int64, float64, bool:
			argValue = fmt.Sprintf("%s: %v", key, v)
		default:
			jsonBytes, _ := json.Marshal(v)
			argValue = fmt.Sprintf("%s: %s", key, string(jsonBytes))
		}
		args = append(args, argValue)
	}
	return strings.Join(args, ", ")
}

// buildDynamicMutation builds a struct type with a single graphql-tagged field for
// the given mutation name and argument string, returning a pointer to an instance
// of that struct. Returns nil if response is not a pointer to a struct.
func buildDynamicMutation(mutationName, args string, response interface{}) interface{} {
	responseVal := reflect.ValueOf(response)
	if responseVal.Kind() != reflect.Pointer {
		return nil
	}
	responseType := responseVal.Elem().Type()

	graphqlTag := fmt.Sprintf(`graphql:"%s(%s)"`, mutationName, args)
	fieldName := strings.ToUpper(mutationName[:1]) + mutationName[1:]

	structType := reflect.StructOf([]reflect.StructField{{
		Name: fieldName,
		Type: responseType,
		Tag:  reflect.StructTag(graphqlTag),
	}})
	return reflect.New(structType).Interface()
}
