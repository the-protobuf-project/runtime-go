module github.com/the-protobuf-project/runtime-go/orm

go 1.26.0

require (
	github.com/glebarez/sqlite v1.11.0
	github.com/google/uuid v1.6.0
	github.com/oklog/ulid/v2 v2.1.1
	github.com/the-protobuf-project/runtime-go/store v0.0.0
	google.golang.org/protobuf v1.36.11
	gorm.io/gorm v1.31.2
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/glebarez/go-sqlite v1.21.2 // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mattn/go-isatty v0.0.21 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	modernc.org/libc v1.22.5 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/sqlite v1.23.1 // indirect
)

replace github.com/the-protobuf-project/runtime-go/store => ../store
