package mongodb

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/machanirobotics/loom/go/mongodb/shared"
)

// DataImporterExporter handles import/export operations.
// It provides methods to export a collection to a CSV file and import
// data from a CSV file into a collection. This interface abstracts the
// underlying MongoDB operations, allowing for easier data management
type DataImporterExporter interface {
	ExportCollectionToCSV(csvFilePath string) error
	ImportCollectionFromCSV(csvFilePath string) error
}

// ExportCollectionToCSV exports all documents from the specified collection
// into a CSV file at csvFilePath. Each document’s field values are converted
// to strings and written as one CSV row.
func (m *MongoDBClient) ExportCollectionToCSV(csvFilePath string) error {
	shared.Pulse.Logger.Debugf("Exporting collection to CSV: collection=%s, path=%s", m.Set.CollectionName, csvFilePath)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cur, err := m.database.Collection(m.Set.CollectionName).Find(ctx, struct{}{})
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to fetch documents for collection %s: %v", m.Set.CollectionName, err)
		return err
	}
	defer cur.Close(ctx)

	file, err := os.Create(csvFilePath)
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to create CSV file %s: %v", csvFilePath, err)
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	rowCount := 0
	for cur.Next(ctx) {
		var record map[string]interface{}
		if err := cur.Decode(&record); err != nil {
			shared.Pulse.Logger.Errorf("Failed to decode document: %v", err)
			return err
		}

		row := make([]string, 0, len(record))
		for _, v := range record {
			row = append(row, fmt.Sprintf("%v", v))
		}
		if err := writer.Write(row); err != nil {
			shared.Pulse.Logger.Errorf("Failed to write CSV row: %v", err)
			return err
		}
		rowCount++
	}

	if err := cur.Err(); err != nil {
		shared.Pulse.Logger.Errorf("Cursor error during export: %v", err)
		return err
	}

	shared.Pulse.Logger.Debugf("Export completed: collection=%s, rows=%d", m.Set.CollectionName, rowCount)
	return nil
}

// ImportCollectionFromCSV reads all rows from the CSV file at csvFilePath
// and inserts them as documents into the specified collection. Each column
// in a row is mapped to fields "field0", "field1", etc.
func (m *MongoDBClient) ImportCollectionFromCSV(csvFilePath string) error {
	shared.Pulse.Logger.Debugf("Importing CSV to collection: collection=%s, path=%s", m.Set.CollectionName, csvFilePath)

	file, err := os.Open(csvFilePath)
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to open CSV file %s: %v", csvFilePath, err)
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to read CSV data from %s: %v", csvFilePath, err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i, row := range rows {
		doc := make(map[string]interface{}, len(row))
		for j, value := range row {
			doc[fmt.Sprintf("field%d", j)] = value
		}
		if _, err := m.database.Collection(m.Set.CollectionName).InsertOne(ctx, doc); err != nil {
			shared.Pulse.Logger.Errorf("Failed to insert CSV row %d: %v", i, err)
			return err
		}
	}

	shared.Pulse.Logger.Debugf("Import completed: collection=%s, rows=%d", m.Set.CollectionName, len(rows))
	return nil
}
