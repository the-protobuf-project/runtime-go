package filterx_test

import (
	"time"

	"github.com/the-protobuf-project/runtime-go/database/filterx"
)

// Genre and Book are a generated bookstore model, as protoc-gen-store's gorm
// target writes one: the fixtures the engines are tested against.
type Genre string

type Book struct {
	ID            string    `gorm:"column:id;primaryKey;not null" json:"id"`
	Name          string    `gorm:"column:name;not null;uniqueIndex" json:"name"`
	Title         string    `gorm:"column:title;not null" json:"title"`
	AuthorID      string    `gorm:"column:author_id;not null" json:"author_id"`
	ISBN          *string   `gorm:"column:isbn;uniqueIndex" json:"isbn,omitempty"`
	PublishedYear *int32    `gorm:"column:published_year" json:"published_year,omitempty"`
	Genre         Genre     `gorm:"column:genre;not null;default:'FICTION'" json:"genre"`
	CreateTime    time.Time `gorm:"column:create_time;type:timestamptz;not null;autoCreateTime" json:"create_time"`
}

func (*Book) TableName() string { return "bookstore_v1.books" }

// BookFilterSpec is the spec protoc-gen-store generates for Book.
var BookFilterSpec = filterx.Spec{
	Table: `"bookstore_v1"."books"`,
	Fields: map[string]filterx.FieldSpec{
		"author_id":      {Column: "author_id", Kind: filterx.KindRef},
		"create_time":    {Column: "create_time", Kind: filterx.KindTimestamp},
		"genre":          {Column: "genre", Kind: filterx.KindEnum, EnumPrefix: "GENRE_"},
		"isbn":           {Column: "isbn", Kind: filterx.KindText},
		"name":           {Column: "name", Kind: filterx.KindText},
		"published_year": {Column: "published_year", Kind: filterx.KindInt},
		"title":          {Column: "title", Kind: filterx.KindText},
	},
	Sort: map[string]filterx.SortSpec{
		"create_time":    {Column: "create_time", Kind: filterx.KindTimestamp, NotNull: true},
		"genre":          {Column: "genre", Kind: filterx.KindEnum, NotNull: true},
		"isbn":           {Column: "isbn", Kind: filterx.KindText},
		"name":           {Column: "name", Kind: filterx.KindText, NotNull: true},
		"published_year": {Column: "published_year", Kind: filterx.KindInt},
		"title":          {Column: "title", Kind: filterx.KindText, NotNull: true},
	},
	PK: []filterx.SortSpec{{Column: "id", Kind: filterx.KindText, NotNull: true}},
}
