package ondatra

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
)

type Connection interface {
	Rebind(sql string) string
	Get(dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	Select(dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context) (*sqlx.Tx, error)
}

type DB struct {
	*sqlx.DB
}

func NewDB(db *sqlx.DB) Connection {
	return &DB{DB: db}
}

func (c *DB) BeginTx(ctx context.Context) (*sqlx.Tx, error) {
	return c.BeginTxx(ctx, nil)
}

type Tx struct {
	*sqlx.Tx
}

func NewTx(tx *sqlx.Tx) Connection {
	return &Tx{Tx: tx}
}

func (c *Tx) BeginTx(_ context.Context) (*sqlx.Tx, error) {
	return nil, ErrAlreadyInTransaction
}
