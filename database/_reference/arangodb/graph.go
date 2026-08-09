package arangodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	arngshd "github.com/arangodb/go-driver/v2/arangodb/shared"
	"github.com/machanirobotics/loom/go/arangodb/shared"
)

// Graph represents the structure of a graph in ArangoDB.
type Graph struct {
	// Name is the unique name of the graph.
	Name string
	// Edges is a list of edge definitions that form the graph's schema.
	Edges []Edge
}

// Edge represents a named edge definition within a graph.
type Edge struct {
	// Name is the name of the edge relation. This often corresponds to the name
	// of the edge collection.
	Name string
	// Definition defines the connections for this edge.
	Definition []EdgeDefinition
	// EdgeManager is an embedded interface for managing edges in the graph.
	Manager EdgeManager
}

// EdgeDefinition specifies the collections that an edge connects.
type EdgeDefinition struct {
	// Collection is the name of the edge collection.
	Collection string
	// From is a list of vertex collection names where edges can originate.
	From []string
	// To is a list of vertex collection names where edges can terminate.
	To []string
}

// GraphManager defines the set of operations for managing graphs in ArangoDB.
// It includes methods for creating, deleting, retrieving, and listing graphs.
// Each operation is performed with a 5-second timeout.
type GraphManager interface {
	// CreateGraph creates a new graph with the specified definition. It ensures that the
	// necessary edge collections exist before creating the graph. If a graph with the
	// same name already exists, an error is returned.
	CreateGraph(input Graph) (*Graph, error)

	// DeleteGraph removes a graph definition by its name. Note that this operation
	// does not delete the associated vertex or edge collections by default.
	// It returns an error if the graph does not exist.
	DeleteGraph(name string) error

	// GetGraph retrieves a graph by its name from the database. It returns an error
	// if the graph cannot be found.
	GetGraph(name string) (*Graph, error)

	// ListGraphs retrieves a list of all graph definitions in the database.
	ListGraphs() ([]*Graph, error)
}

// CreateGraph creates a new graph with the specified definition.
// It first ensures that the necessary edge collections exist, creating them if they don't.
// It returns a Graph object representing the newly created graph or an error if the
// operation fails or the graph already exists.
func (g *arangoInterfaceWrapper) CreateGraph(input Graph) (*Graph, error) {
	shared.Pulse.Logger.Debug("Entering CreateGraph", input.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, edge := range input.Edges {
		coll, err := g.checkIfCollectionExistsAndCreateEdge(ctx, edge.Name)
		if err != nil {
			shared.Pulse.Logger.Errorf("Failed to check or create collection '%s': %v", edge.Name, err)
			return nil, fmt.Errorf("failed to check or create collection '%s': %w", edge.Name, err)
		}
		shared.Pulse.Logger.Debugf("Collection checked/created successfully: %s", coll.Name())
	}

	graph, err := g.Database.CreateGraph(context.Background(), input.Name, &arangodb.GraphDefinition{
		Name:            input.Name,
		EdgeDefinitions: g.parseEdgeDefinitions(input.Edges),
	}, nil)
	if err != nil {
		shared.Pulse.Logger.Error("Failed to create graph", fmt.Sprintf("name: %s, error: %v", input.Name, err))
		return nil, fmt.Errorf("failed to get graph: %w", err)
	}

	graphObj := g.toGraphObject(graph, input)
	if graphObj == nil {
		shared.Pulse.Logger.Error("Failed to convert arangodb.Graph to Graph type", "name", input.Name)
		return nil, fmt.Errorf("failed to convert arangodb.Graph to Graph type for name: %s", input.Name)
	}
	shared.Pulse.Logger.Debugf("Graph converted successfully: %s", graphObj.Name)
	return graphObj, nil
}

// DeleteGraph removes the graph definition with the given name.
// This function only removes the graph definition itself; it does not delete
// the vertex or edge collections associated with it.
func (g *arangoInterfaceWrapper) DeleteGraph(name string) error {
	shared.Pulse.Logger.Debugf("Entering DeleteGraph for: %s", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	graph, err := g.Database.Graph(ctx, name, nil)
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to get graph '%s': %v", name, err)
		return fmt.Errorf("failed to get graph '%s': %w", name, err)
	}

	if err := graph.Remove(ctx, nil); err != nil {
		shared.Pulse.Logger.Errorf("Failed to delete graph '%s': %v", name, err)
		return fmt.Errorf("failed to delete graph '%s': %w", name, err)
	}
	shared.Pulse.Logger.Debugf("Graph '%s' deleted successfully", name)
	return nil
}

// GetGraph retrieves a graph by its name from the database.
// It returns a Graph object populated with the graph's definition or an error
// if the graph is not found or another database error occurs.
func (g *arangoInterfaceWrapper) GetGraph(name string) (*Graph, error) {
	shared.Pulse.Logger.Debugf("Entering GetGraph for: %s", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	graph, err := g.Database.Graph(ctx, name, nil)
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to get graph '%s': %v", name, err)
		return nil, fmt.Errorf("failed to get graph '%s': %w", name, err)
	}

	defns := graph.EdgeDefinitions()
	graphObj := g.toGraphObject(graph, Graph{
		Name:  graph.Name(),
		Edges: g.toEdgeObject(defns),
	})
	if graphObj == nil {
		shared.Pulse.Logger.Errorf("Failed to convert arangodb.Graph to Graph type: name=%s", name)
		return nil, fmt.Errorf("failed to convert arangodb.Graph to Graph type for name: %s", name)
	}
	shared.Pulse.Logger.Infof("Retrieved graph successfully: %+v", graphObj)
	return graphObj, nil
}

// ListGraphs retrieves all graph definitions from the database.
// It returns a slice of Graph pointers or an error if the list cannot be retrieved.
func (g *arangoInterfaceWrapper) ListGraphs() ([]*Graph, error) {
	shared.Pulse.Logger.Debug("Entering ListGraphs")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	graphReader, err := g.Graphs(ctx)
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to list graphs: %v", err)
		return nil, fmt.Errorf("failed to list graphs: %w", err)
	}

	var graphList []*Graph
	for {
		graph, err := graphReader.Read()
		if errors.Is(err, arngshd.NoMoreDocumentsError{}) {
			break
		}
		if err != nil {
			shared.Pulse.Logger.Errorf("Error while reading graph: %v", err)
			break
		}

		edges := g.toEdgeObject(graph.EdgeDefinitions())
		shared.Pulse.Logger.Debugf("Graph '%s' has %d edge(s)", graph.Name(), len(edges))

		graphList = append(graphList, &Graph{
			Name:  graph.Name(),
			Edges: edges,
		})
	}

	shared.Pulse.Logger.Infof("Listed %d graphs successfully", len(graphList))
	return graphList, nil
}
