package dbproxy

import (
	sd "github.com/rosbit/go-sql-driver"
	"database/sql/driver"
	"fmt"
	"os"
)

const (
	http_db_proxy_env = "HTTP_DB_PROXY_BASEURL"
)

var (
	baseURL string
	httpDriverInited bool
	httpDriver *HttpDBProxyDriver
)

func initHttpDriver() error {
	if httpDriverInited {
		return nil
	}
	baseURL = os.Getenv(http_db_proxy_env)
	if len(baseURL) == 0 {
		return fmt.Errorf("env %s expected", http_db_proxy_env)
	}
	httpDriver = &HttpDBProxyDriver{}
	sd.Register(httpDriver)
	httpDriverInited = true
	return nil
}

type httpDB struct {
	dbId string
	txId string
}

type httpStmt struct {
	*httpDB
	stmtId string
}

type httpTx struct {
	*httpDB
}

// --------  a driver implementation  -----------
type HttpDBProxyDriver struct {
}

func (d *HttpDBProxyDriver) GetDriverName() string {
	return driver_name
}

func (d *HttpDBProxyDriver) CreateConnection(dsn string) (dbId interface{}, err error) {
	// fmt.Printf("CreateConnection(%s) called\n", dsn)
	var res struct {
		DbId string `json:"db-id"`
	}
	if _, _, err = newHttpDBProxyCaller().callDBProxy("open-db", map[string]interface{}{"dsn": dsn}, &res); err != nil {
		return
	}
	dbId = &httpDB{
		dbId: res.DbId,
	}
	return
}

func (d *HttpDBProxyDriver) CloseConnection(dbInst interface{}) (err error) {
	// fmt.Printf("CloseConnection(%v) called\n", dbInst)
	c := dbInst.(*httpDB)
	_, _, err = newHttpDBProxyCaller().callDBProxy("close-db", map[string]interface{}{"did": c.dbId}, nil)
	return
}

func (d *HttpDBProxyDriver) Ping(dbInst interface{}) (err error) {
	c := dbInst.(*httpDB)
	_, _, err = newHttpDBProxyCaller().callDBProxy("ping", map[string]interface{}{
		"did": c.dbId,
		"tid": c.txId,
	}, nil)
	return
}

func (d *HttpDBProxyDriver) BeginTx(dbInst interface{}, opts driver.TxOptions) (txId interface{}, err error) {
	// fmt.Printf("BeginTx called\n")
	c := dbInst.(*httpDB)
	var res struct {
		TxId string `json:"tx-id"`
	}
	if _, _, err = newHttpDBProxyCaller().callDBProxy("begin-tx", map[string]interface{}{
		"did": c.dbId,
		"opts": opts,
	}, &res); err != nil {
		return
	}
	c.txId = res.TxId
	txId = &httpTx{
		httpDB: c,
	}
	return
}

func (d *HttpDBProxyDriver) Commit(tx interface{}) (err error) {
	// fmt.Printf("Commit called\n")
	t := tx.(*httpTx)
	_, _, err = newHttpDBProxyCaller().callDBProxy("commit", map[string]interface{}{"tid": t.txId}, nil)
	t.httpDB.txId = ""
	return
}

func (d *HttpDBProxyDriver) Rollback(tx interface{}) (err error) {
	// fmt.Printf("Rollback called\n")
	t := tx.(*httpTx)
	_, _, err = newHttpDBProxyCaller().callDBProxy("rollback", map[string]interface{}{"tid": t.txId}, nil)
	t.httpDB.txId = ""
	return
}

func (d *HttpDBProxyDriver) Prepare(dbInst interface{}, query string) (stmtId interface{}, err error) {
	// fmt.Printf("Prepare %s\n", query)
	c := dbInst.(*httpDB)
	var res struct {
		StmtId string `json:"stmt-id"`
	}
	if _, _, err = newHttpDBProxyCaller().callDBProxy("prepare", map[string]interface{}{
		"did": c.dbId,
		"query": query,
		"tid": c.txId,
	}, &res); err != nil {
		return
	}
	stmtId = &httpStmt{
		httpDB: c,
		stmtId: res.StmtId,
	}
	return
}

func (d *HttpDBProxyDriver) CloseStmt(stmt interface{}) (err error) {
	// fmt.Printf("close stmt(%v) called\n", stmt)
	s := stmt.(*httpStmt)
	_, _, err = newHttpDBProxyCaller().callDBProxy("close-stmt", map[string]interface{}{"sid": s.stmtId}, nil)
	return
}

func (d *HttpDBProxyDriver) Exec(stmt interface{}, args ...interface{}) (ec sd.ExecResult, err error) {
	// fmt.Printf("exec stmt(%v) called\n", stmt)
	s := stmt.(*httpStmt)
	var res struct {
		LastInsertId int64 `json:"lastInsertId"`
		RowsAffected int64 `json:"rowsAffected"`
	}
	if _, _, err = newHttpDBProxyCaller().callDBProxy("exec", map[string]interface{}{
		"sid": s.stmtId,
		"args": args,
	}, &res); err != nil {
		return
	}
	ec = &Result{
		lastInsertId: res.LastInsertId,
		rowsAffected: res.RowsAffected,
	}
	return
}

func (d *HttpDBProxyDriver) Query(stmt interface{}, args ...interface{}) (r sd.ResultSet, err error) {
	// fmt.Printf("query stmt(%v) called\n", stmt)
	s := stmt.(*httpStmt)
	var res struct {
		Columns []string `json:"columns"`
	}
	_, jsonl, e := newHttpDBProxyCaller().callDBProxy("query", map[string]interface{}{
		"sid": s.stmtId,
		"args": args,
	}, &res);
	if e != nil {
		err = e
		return
	}
	r = &ResultSet{
		columns: res.Columns,
		rows: jsonl,
	}
	return
}

