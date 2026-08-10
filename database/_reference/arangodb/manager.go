package arangodb

import "github.com/arangodb/go-driver/v2/arangodb"

// ArangoDBManager provides a unified API for interacting with different aspects
// of a specific ArangoDB database. It acts as a primary entry point, holding
// specialized managers for collections, documents, and graphs.
type ArangoDBManager struct {
	Graph      GraphManager      // GraphManager provides methods for managing graphs in the database.
	Collection CollectionManager // CollectionManager provides methods for managing collections in the database.
	Document   DocumentManager   // DocumentManager provides methods for managing documents within a specific collection.
}

// arangoInterfaceWrapper is an internal struct that implements the various manager
// interfaces (e.g., CollectionManager, DocumentManager). It embeds types from the
// official ArangoDB driver to gain direct access to the underlying database,
// collection, and graph objects needed to perform operations.
type arangoInterfaceWrapper struct {
	arangodb.Database   // The underlying ArangoDB database object.
	arangodb.Collection // The specific collection within the database that this manager operates on.
	arangodb.Graph      // 	The specific graph within the database that this manager operates on.
}
