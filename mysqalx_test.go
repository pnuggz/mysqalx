package mysqalx_test

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pnuggz/mysqalx"
	"github.com/pnuggz/mysqalx/schemas"
)

func createNode(t *testing.T) (*sqlx.DB, mysqalx.MyTx) {
	dataSource := "mysqalx:mysqalx@tcp(localhost:3306)/mysqalx"

	db, err := sqlx.Connect("mysql", dataSource)
	if err != nil {
		t.Errorf("%s", err)
	}

	node, _ := mysqalx.New(db, mysqalx.SavePoint(true))

	for _, schema := range schemas.GetAllSchema() {
		db.MustExec(schema)
	}

	db.Query("truncate t1")
	db.Query("truncate t2")
	db.Query("truncate t3")

	return db, node
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

type T3 struct {
	ID string `json:"id" db:"id"`
}

func TestSingleCommit(t *testing.T) {
	db, node := createNode(t)

	ctx := context.Background()

	txService, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Commit()

	txService.Commit()

	query := "select * from t1"
	rows, err := db.QueryContext(ctx, query)
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
}

func TestSingleRollback(t *testing.T) {
	db, node := createNode(t)

	ctx := context.Background()

	txService, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Rollback()

	txService.Commit()

	query := "select * from t1"
	rows, err := db.QueryContext(ctx, query)
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
}

func TestSingleCommitAndSingleRollback(t *testing.T) {
	db, node := createNode(t)

	ctx := context.Background()

	txService, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Commit()

	tx2, _ := node.BeginTxx(ctx, nil)
	tx2.ExecContext(ctx, "INSERT INTO t2(id) VALUES('abc')")
	tx2.Rollback()

	txService.Commit()

	query := "select * from t1"
	rows, err := db.QueryContext(ctx, query)
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
	rows, err = db.QueryContext(ctx, query)
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
}

func TestDoubleCommitAndSingleRollback(t *testing.T) {
	db, node := createNode(t)

	ctx := context.Background()

	txService, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Commit()

	tx2, _ := node.BeginTxx(ctx, nil)
	tx2.ExecContext(ctx, "INSERT INTO t2(id) VALUES('abc')")
	tx2.Commit()

	tx3, _ := node.BeginTxx(ctx, nil)
	tx3.ExecContext(ctx, "INSERT INTO t3(id) VALUES('abc')")
	tx3.Rollback()

	txService.Commit()

	query := "select * from t1"
	rows, err := db.QueryContext(ctx, query)
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
	rows, err = db.QueryContext(ctx, query)
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

	if len(res2) == 0 {
		t.Errorf("commit didn't work")
	}

	query = "select * from t3"
	rows, err = db.QueryContext(ctx, query)
	if err != nil {
		t.Errorf(err.Error())
	}

	res3 := []T3{}
	for rows.Next() {
		var cm T3
		err := rows.Scan(&cm.ID)
		if err != nil {
			t.Errorf(err.Error())
		}

		res3 = append(res3, cm)
	}

	if len(res3) != 0 {
		t.Errorf("rollback didn't work")
	}
}

func TestDoubleCommitAndSingleRollbackAndAllRollback(t *testing.T) {
	db, node := createNode(t)

	ctx := context.Background()

	txService, _ := node.BeginTxx(ctx, nil)

	tx1, _ := node.BeginTxx(ctx, nil)
	tx1.ExecContext(ctx, "INSERT INTO t1(id) VALUES('abc')")
	tx1.Commit()

	tx2, _ := node.BeginTxx(ctx, nil)
	tx2.ExecContext(ctx, "INSERT INTO t2(id) VALUES('abc')")
	tx2.Commit()

	tx3, _ := node.BeginTxx(ctx, nil)
	tx3.ExecContext(ctx, "INSERT INTO t3(id) VALUES('abc')")
	tx3.Rollback()

	txService.Rollback()

	query := "select * from t1"
	rows, err := db.QueryContext(ctx, query)
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

	query = "select * from t2"
	rows, err = db.QueryContext(ctx, query)
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

	query = "select * from t3"
	rows, err = db.QueryContext(ctx, query)
	if err != nil {
		t.Errorf(err.Error())
	}

	res3 := []T3{}
	for rows.Next() {
		var cm T3
		err := rows.Scan(&cm.ID)
		if err != nil {
			t.Errorf(err.Error())
		}

		res3 = append(res3, cm)
	}

	if len(res3) != 0 {
		t.Errorf("rollback didn't work")
	}
}
