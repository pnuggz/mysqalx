package mysqalx

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/rogpeppe/fastuuid"
)

var (
	// ErrNotInTransaction is returned when using Commit
	// outside of a transaction.
	ErrNotInTransaction = errors.New("not in transaction")

	// ErrIncompatibleOption is returned when using an option incompatible
	// with the selected driver.
	ErrIncompatibleOption = errors.New("incompatible option")
)

var uuids = fastuuid.MustNewGenerator()

// A Node is a database driver that can manage nested transactions.
type MyTx interface {
	Driver

	// Close the underlying sqlx connection.
	Close() error
	// Begin a new transaction.
	Beginx() (MyTx, error)
	// Begin a new transaction using the provided context and options.
	// Note that the provided parameters are only used when opening a new transaction,
	// not on nested ones.
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (MyTx, error)
	// Rollback the associated transaction.
	Rollback() error
	// Commit the assiociated transaction.
	Commit() error
	// Tx returns the underlying transaction.
	Tx() *sqlx.Tx
}

// A Driver can query the database. It can either be a *sqlx.DB or a *sqlx.Tx
// and therefore is limited to the methods they have in common.
type Driver interface {
	sqlx.Execer
	sqlx.ExecerContext
	sqlx.Queryer
	sqlx.QueryerContext
	sqlx.Preparer
	sqlx.PreparerContext
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	DriverName() string
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	MustExec(query string, args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRow(string, ...interface{}) *sql.Row
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// New creates a new Node with the given DB.
func New(db *sqlx.DB, options ...Option) (MyTx, error) {
	n := &myTx{
		db:     db,
		Driver: db,
	}

	for _, opt := range options {
		err := opt(n)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// Connect to a database.
func Connect(driverName, dataSourceName string, options ...Option) (MyTx, error) {
	db, err := sqlx.Connect(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	node, err := New(db, options...)
	if err != nil {
		// the connection has been opened within this function, we must close it
		// on error.
		db.Close()
		return nil, err
	}

	return node, nil
}

type myTx struct {
	Driver
	db               *sqlx.DB
	tx               *sqlx.Tx
	savePointIDs     []string
	savePointEnabled bool
	transactionCount int
	commitCount      int
}

func (n *myTx) Close() error {
	return n.db.Close()
}

func (n myTx) Beginx() (MyTx, error) {
	return n.BeginTxx(context.Background(), nil)
}

func (n *myTx) BeginTxx(ctx context.Context, opts *sql.TxOptions) (MyTx, error) {
	var err error

	if n.tx == nil {
		// new actual transaction
		n.tx, err = n.db.BeginTxx(ctx, opts)
		n.Driver = n.tx
		if err != nil {
			return nil, err
		}
	}

	n.transactionCount += 1

	// savepoints name must start with a char and cannot contain dashes (-)
	savePointID := "sp_" + strings.Replace(uuids.Hex128(), "-", "_", -1)
	n.savePointIDs = append(n.savePointIDs, savePointID)
	_, err = n.tx.Exec("SAVEPOINT " + savePointID)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *myTx) Rollback() error {
	if n.tx == nil {
		return nil
	}

	n.transactionCount -= 1

	var err error

	// if we are not at the top level then
	// just rollback to the previous level
	if n.transactionCount != n.commitCount {
		savePointID := n.savePointIDs[len(n.savePointIDs)-1]
		_, err = n.tx.Exec("ROLLBACK TO " + savePointID)
		if err != nil {
			return err
		}
		n.savePointIDs = n.savePointIDs[:len(n.savePointIDs)-1]
		return nil
	}

	err = n.tx.Rollback()
	if err != nil {
		return err
	}

	n.tx = nil
	n.Driver = nil

	return nil
}

func (n *myTx) Commit() error {
	if n.tx == nil {
		return ErrNotInTransaction
	}

	var err error

	n.commitCount += 1

	// If this is not the final commit, then
	// we just create a new savepoint
	if n.transactionCount != n.commitCount {
		// savepoints name must start with a char and cannot contain dashes (-)
		savePointID := "sp_" + strings.Replace(uuids.Hex128(), "-", "_", -1)
		n.savePointIDs = append(n.savePointIDs, savePointID)
		_, err = n.tx.Exec("SAVEPOINT " + savePointID)
		if err != nil {
			return err
		}

		return nil
	}

	err = n.tx.Commit()
	if err != nil {
		return err
	}

	n.tx = nil
	n.Driver = nil

	return nil
}

// Tx returns the underlying transaction.
func (n *myTx) Tx() *sqlx.Tx {
	return n.tx
}

// Option to configure sqalx
type Option func(*myTx) error

// SavePoint option enables PostgreSQL and SQLite Savepoints for nested
// transactions.
func SavePoint(enabled bool) Option {
	return func(n *myTx) error {
		driverName := n.Driver.DriverName()
		if enabled && driverName != "mysql" {
			return ErrIncompatibleOption
		}
		n.savePointEnabled = enabled
		return nil
	}
}
