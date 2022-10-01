package mysqalx_test

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pnuggz/mysqalx"
	"github.com/pnuggz/mysqalx/schemas"
)

func createNode(t *testing.T) mysqalx.MyTx {
	dataSource := "mysqalx:mysqalx@tcp(localhost:3307)/mysqalx"

	db, err := sqlx.Connect("mysql", dataSource)
	if err != nil {
		t.Errorf("%s", err)
	}

	node, _ := mysqalx.New(db, mysqalx.SavePoint(true))

	for _, schema := range schemas.GetAllSchema() {
		db.MustExec(schema)
	}

	return node
}

func TestMysqalxConnectMySQL(t *testing.T) {
	createNode(t)
}

type T1 struct {
	ID string `json:"id" db:"id"`
}

type T2 struct {
	ID string `json:"id" db:"id"`
}

func TestSingleCommit(t *testing.T) {
	node := createNode(t)

	ctx := context.Background()

	txTest, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Commit()

	query := "select * from t1"
	rows, err := tx1.QueryContext(ctx, query)
	if err != nil {
		t.Errorf(err.Error())
	}

	res := []T1{}

	for rows.Next() {
		var c T1
		err := rows.Scan(&c.ID)
		if err != nil {
			t.Errorf(err.Error())
		}

		res = append(res, c)
	}

	if len(res) == 0 {
		t.Errorf("commit didn't work")
	}

	txTest.Rollback()
}

func TestSingleRollback(t *testing.T) {
	node := createNode(t)

	ctx := context.Background()

	txTest, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Rollback()

	query := "select * from t1"
	rows, err := tx1.QueryContext(ctx, query)
	if err != nil {
		t.Errorf(err.Error())
	}

	res := []T1{}

	for rows.Next() {
		var c T1
		err := rows.Scan(&c.ID)
		if err != nil {
			t.Errorf(err.Error())
		}

		res = append(res, c)
	}

	if len(res) != 0 {
		t.Errorf("rollback didn't work")
	}

	txTest.Rollback()
}

func TestSingleCommitAndSingleRollback(t *testing.T) {
	node := createNode(t)

	ctx := context.Background()

	txTest, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Commit()

	tx2, _ := node.BeginTxx(ctx, nil)
	tx2.ExecContext(ctx, "INSERT INTO t2(id) VALUES('abc')")
	tx2.Rollback()

	query := "select * from t1"
	rows, err := tx1.QueryContext(ctx, query)
	if err != nil {
		t.Errorf(err.Error())
	}

	res := []T1{}

	for rows.Next() {
		var c T1
		err := rows.Scan(&c.ID)
		if err != nil {
			t.Errorf(err.Error())
		}

		res = append(res, c)
	}

	if len(res) == 0 {
		t.Errorf("commit didn't work")
	}

	query = "select * from t2"
	rows, err = tx2.QueryContext(ctx, query)
	if err != nil {
		t.Errorf(err.Error())
	}

	res2 := []T2{}
	for rows.Next() {
		var cm T2
		err := rows.Scan(&cm.ID)
		if err != nil {
			t.Errorf(err.Error())
		}

		res2 = append(res2, cm)
	}

	if len(res2) != 0 {
		t.Errorf("rollback didn't work")
	}

	txTest.Rollback()
}
