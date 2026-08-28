module github.com/the-protobuf-project/runtime-go/database

go 1.26.4

require (
	github.com/arangodb/go-driver/v2 v2.3.1
	github.com/glebarez/sqlite v1.11.0
	github.com/google/uuid v1.6.0
	github.com/oklog/ulid/v2 v2.1.2
	github.com/redis/go-redis/v9 v9.18.0
	github.com/the-protobuf-project/runtime-go/cache v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7
	go.mongodb.org/mongo-driver/v2 v2.8.0
	google.golang.org/protobuf v1.36.12
	gorm.io/driver/mysql v1.6.0
	gorm.io/driver/postgres v1.6.2
	gorm.io/gorm v1.31.2
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/arangodb/go-velocypack v0.0.0-20200318135517-5af53c29c67e // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dchest/siphash v1.2.3 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/glebarez/go-sqlite v1.21.2 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/kkdai/maglev v0.2.0 // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-00010101000000-000000000000 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	modernc.org/libc v1.22.5 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/sqlite v1.23.1 // indirect
)

replace github.com/the-protobuf-project/runtime-go/cache => ../cache

// The cache module this one composes with pulls these in, and a replace
// directive does not transit from a dependency — so the paths are repeated
// here for the same three sibling modules.
replace github.com/the-protobuf-project/runtime-go/observability => ../observability

replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../ulid
