// Transactional multi-mutation batching.
//
// A single GraphQL mutation document may carry several top-level mutation fields; an engine
// like Hasura executes them in ONE transaction (all commit or all roll back). BatchMutate
// builds such a document from several BatchOps so a write spanning tables commits atomically,
// replacing best-effort compensation in application code.
package network

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// BatchOp is one mutation in a transactional batch: a root mutation field, its arguments
// (values are graphql.Var/VarPtr wrappers carrying their GraphQL types), and a pointer to the
// typed response it fills on success.
type BatchOp struct {
	Field  string
	Args   map[string]interface{}
	Result interface{} // pointer to the typed response; filled when the batch commits
}

// BatchMutate runs every op as one GraphQL mutation document, executed by the engine in a
// single transaction. Each op's arguments are namespaced (m0_, m1_, ...) so fields sharing an
// argument name (e.g. two inserts with "objects") do not collide, and each op's selection is
// aliased so the responses decode back into the per-op Result pointers. The returned channel
// receives one GraphQLResult and is closed; on error no Result pointer is written.
func (g *GraphQLClient) BatchMutate(ctx context.Context, ops []BatchOp) <-chan GraphQLResult {
	resultChan := make(chan GraphQLResult, 1)
	go func() {
		defer close(resultChan)
		if g.client == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("GraphQL client is not initialized")}
			return
		}
		if len(ops) == 0 {
			resultChan <- GraphQLResult{}
			return
		}
		fields := make([]reflect.StructField, len(ops))
		variables := map[string]interface{}{}
		for i, op := range ops {
			rv := reflect.ValueOf(op.Result)
			if rv.Kind() != reflect.Pointer || rv.IsNil() {
				resultChan <- GraphQLResult{Error: fmt.Errorf("batch op %d (%s): Result must be a non-nil pointer", i, op.Field)}
				return
			}
			alias := "m" + strconv.Itoa(i)
			fields[i] = reflect.StructField{
				Name: "F" + strconv.Itoa(i),
				Type: rv.Type().Elem(),
				Tag:  reflect.StructTag(fmt.Sprintf("graphql:%q", buildBatchTag(alias, op.Field, op.Args))),
			}
			for k, v := range op.Args {
				variables[alias+"_"+k] = v
			}
		}
		wrapper := reflect.New(reflect.StructOf(fields))

		ctx, cancel := context.WithTimeout(ctx, g.Timeout)
		defer cancel()
		if err := g.client.Mutate(ctx, wrapper.Interface(), variables); err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to execute batch mutation: %w", err)}
			return
		}
		for i, op := range ops {
			reflect.ValueOf(op.Result).Elem().Set(wrapper.Elem().Field(i))
		}
		resultChan <- GraphQLResult{Response: wrapper.Interface()}
	}()
	return resultChan
}

// buildBatchTag renders one batched field as `alias: field(arg: $alias_arg, ...)` (arguments
// sorted for determinism, variables namespaced by alias), or `alias: field` when it has none.
func buildBatchTag(alias, field string, args map[string]interface{}) string {
	if len(args) == 0 {
		return fmt.Sprintf("%s: %s", alias, field)
	}
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s: $%s_%s", k, alias, k))
	}
	return fmt.Sprintf("%s: %s(%s)", alias, field, strings.Join(parts, ", "))
}
