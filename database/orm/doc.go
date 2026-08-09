// Package orm implements the backend-agnostic database.Driver over GORM. It is the
// reference relational driver: a single dynamic engine that runs CRUD for every
// resource using GORM's map + Table API, so no per-resource Go model types are
// needed (the dynamic counterpart of protorm's statically-typed generated
// gormx.GenericStore[M], which stays available for compile-time-typed access).
//
// The *gorm.DB passed to New should be opened with gorm.Config{TranslateError:
// true} so duplicate-key and not-found driver errors are reported as the GORM
// sentinels this driver maps to database.ErrAlreadyExists / database.ErrNotFound.
//
// # Example
//
// Open a *gorm.DB, then serve the generated proto API over SQL — no per-resource
// model types, one dynamic engine for every resource:
//
//	db, _ := gorm.Open(sqlite.Open("app.db"), &gorm.Config{TranslateError: true})
//	reg := database.NewRegistry(grpcx.Resources...) // descriptors from target=grpc
//	svc := adapter.New(orm.New(db), reg)          // wire svc into your gRPC server
//
// # Selecting a schema, and the capabilities beyond CRUD
//
// [NewProvider] adds the step before CRUD and is what a multi-tenant program
// wants: the same descriptors, a schema chosen per request or per tenant.
//
//	p := orm.NewProvider(db)
//	tenant, _ := p.SetDatabase(ctx, "tenant_a")
//	defer tenant.Close()
//
//	tenant.Schema.EnsureSchema(ctx, res)  // CREATE TABLE from the descriptor
//	tenant.Tx.Run(ctx, func(tx *database.DB) error {
//	    _, err := tx.Create(ctx, res, book)
//	    return err
//	})
//
// The name qualifies every table this driver targets, overriding the schema each
// resource was generated with. It is checked against an identifier allowlist
// before it reaches a statement — in a multi-tenant program the name arrives
// from a request, which is exactly the path an injected identifier travels.
//
// This driver implements [database.Transactional] and [database.Migrator]. The client
// is yours: this package does not open it and does not close it, so
// [database.DB.Close] is a no-op here.
package orm
