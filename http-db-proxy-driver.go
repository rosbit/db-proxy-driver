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
	httpDriver = &HttpDBProxyDriver{
		dbProxyDriverAdapter: &dbProxyDriverAdapter{
			dbProxyCaller: _httpDBProxyCaller,
		},
	}
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
	*dbProxyDriverAdapter
}

func (d *HttpDBProxyDriver) GetDriverName() string {
	return driver_name
}

func (d *HttpDBProxyDriver) CreateConnection(dsn string) (dbInst interface{}, err error) {
	// fmt.Printf("CreateConnection(%s) called\n", dsn)
	var dbId string
	if dbId, err = d.openDB(dsn); err != nil {
		return
	}
	dbInst = &httpDB{
		dbId: dbId,
	}
	return
}

func (d *HttpDBProxyDriver) CloseConnection(dbInst interface{}) (err error) {
	// fmt.Printf("CloseConnection(%v) called\n", dbInst)
	c := dbInst.(*httpDB)
	return d.closeDB(c.dbId)
}

func (d *HttpDBProxyDriver) Ping(dbInst interface{}) (err error) {
	c := dbInst.(*httpDB)
	return d.ping(c.dbId, c.txId)
}

func (d *HttpDBProxyDriver) BeginTx(dbInst interface{}, opts driver.TxOptions) (txId interface{}, err error) {
	// fmt.Printf("BeginTx called\n")
	c := dbInst.(*httpDB)
	var tId string
	if tId, err = d.beginTx(c.dbId, opts); err != nil {
		return
	}
	c.txId = tId
	txId = &httpTx{
		httpDB: c,
	}
	return
}

func (d *HttpDBProxyDriver) Commit(tx interface{}) (err error) {
	// fmt.Printf("Commit called\n")
	t := tx.(*httpTx)
	err = d.commit(t.txId)
	t.httpDB.txId = ""
	return
}

func (d *HttpDBProxyDriver) Rollback(tx interface{}) (err error) {
	// fmt.Printf("Rollback called\n")
	t := tx.(*httpTx)
	err = d.rollback(t.txId)
	t.httpDB.txId = ""
	return
}

func (d *HttpDBProxyDriver) Prepare(dbInst interface{}, query string) (stmtId interface{}, err error) {
	// fmt.Printf("Prepare %s\n", query)
	c := dbInst.(*httpDB)
	var sId string
	if sId, err = d.prepare(c.dbId, c.txId, query); err != nil {
		return
	}
	stmtId = &httpStmt{
		httpDB: c,
		stmtId: sId,
	}
	return
}

func (d *HttpDBProxyDriver) CloseStmt(stmt interface{}) (err error) {
	// fmt.Printf("close stmt(%v) called\n", stmt)
	s := stmt.(*httpStmt)
	return d.closeStmt(s.stmtId)
}

func (d *HttpDBProxyDriver) Exec(stmt interface{}, args ...interface{}) (ec sd.ExecResult, err error) {
	// fmt.Printf("exec stmt(%v) called\n", stmt)
	s := stmt.(*httpStmt)
	return d.exec(s.stmtId, args...)
}

func (d *HttpDBProxyDriver) Query(stmt interface{}, args ...interface{}) (r sd.ResultSet, err error) {
	// fmt.Printf("query stmt(%v) called\n", stmt)
	s := stmt.(*httpStmt)
	return d.query(s.stmtId, args...)
}

