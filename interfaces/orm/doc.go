// Package orm implements the backend-agnostic store.Driver over GORM. It is the
// reference relational driver: a single dynamic engine that runs CRUD for every
// resource using GORM's map + Table API, so no per-resource Go model types are
// needed (the dynamic counterpart of protorm's statically-typed generated
// gormx.GenericStore[M], which stays available for compile-time-typed access).
//
// The *gorm.DB passed to New should be opened with gorm.Config{TranslateError:
// true} so duplicate-key and not-found driver errors are reported as the GORM
// sentinels this driver maps to store.ErrAlreadyExists / store.ErrNotFound.
//
// # Example
//
// Open a *gorm.DB, then serve the generated proto API over SQL — no per-resource
// model types, one dynamic engine for every resource:
//
//	db, _ := gorm.Open(sqlite.Open("app.db"), &gorm.Config{TranslateError: true})
//	reg := store.NewRegistry(grpcx.Resources...) // descriptors from target=grpc
//	svc := adapter.New(orm.New(db), reg)          // wire svc into your gRPC server
package orm
