// The examples are their own module.
//
// Two reasons. They import the database module by its published path rather
// than by a relative one, so they exercise what an outside consumer actually
// writes — an example that reached into the package next door would compile
// under conditions no user has. And they are commands rather than library code:
// keeping them out of the module means `go build ./...` on the library never
// depends on whether a demo still compiles.
module github.com/the-protobuf-project/runtime-go/database/examples

go 1.26.4

require (
	github.com/glebarez/sqlite v1.11.0
	github.com/redis/go-redis/v9 v9.23.0
	github.com/the-protobuf-project/runtime-go/cache v0.0.0-20261023180446-a76d56a9756b
	github.com/the-protobuf-project/runtime-go/database v0.0.0
	google.golang.org/protobuf v1.36.12
	gorm.io/driver/postgres v1.6.3
	gorm.io/gorm v1.31.2
)

require (
	github.com/arangodb/go-driver/v2 v2.4.1 // indirect
	github.com/arangodb/go-velocypack v0.0.0-20200318135517-5af53c29c67e // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dchest/siphash v1.2.3 // indirect
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/glebarez/go-sqlite v1.23.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/kkdai/maglev v0.2.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/mattn/go-colorable v0.1.16 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20261024035205-2d2b451ed122 // indirect
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-20261023180446-a76d56a9756b // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.mongodb.org/mongo-driver/v2 v2.9.2 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	modernc.org/libc v1.79.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
	modernc.org/sqlite v1.61.0 // indirect
)

// Every sibling resolves from the tree: none of them has a published tag of its
// own, and they are versioned together with the rest of runtime-go.
replace github.com/the-protobuf-project/runtime-go/database => ../

replace github.com/the-protobuf-project/runtime-go/cache => ../../cache

replace github.com/the-protobuf-project/runtime-go/observability => ../../observability

replace github.com/the-protobuf-project/runtime-go/telemetry => ../../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../../ulid
