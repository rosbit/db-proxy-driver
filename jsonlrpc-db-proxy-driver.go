package dbproxy

import (
	sd "github.com/rosbit/go-sql-driver"
	"github.com/rosbit/jsonl-rpc"
	"database/sql/driver"
	"fmt"
	"os"
	"net/rpc"
)

const (
	jsonlrpc_db_proxy_env  = "JSONLRPC_DB_PROXY_HOST"
	websocket_db_proxy_env = "JSONLRPC_DB_PROXY_WS_HOST"
)

var (
	jsonlProxyHost string
	jsonlDrivedInited bool
	jsonlDriver *JSONLDBProxyDriver
)

func initJSONLRpcDriver() error {
	if jsonlDrivedInited {
		return nil
	}
	var usingWebsocket bool
	jsonlProxyHost = os.Getenv(jsonlrpc_db_proxy_env)
	if len(jsonlProxyHost) == 0 {
		jsonlProxyHost = os.Getenv(websocket_db_proxy_env)
		if len(jsonlProxyHost) == 0 {
			return fmt.Errorf("env %s or %s expected", jsonlrpc_db_proxy_env, websocket_db_proxy_env)
		}
		usingWebsocket = true
	}
	jsonlDriver = &JSONLDBProxyDriver{
		dbProxyDriverAdapter: &dbProxyDriverAdapter{},
	}
	sd.Register(jsonlDriver)
	jsonlDrivedInited = true
	if usingWebsocket {
		if err := os.Setenv("WEBSOCKET_ENDPOINT", "/websocket"); err != nil {
			return err
		}
	}
	return nil
}

type rpcDB struct {
	dbProxyClient **rpc.Client
	dbId string
	txId string
	reconnect func()error
}

type rpcStmt struct {
	*rpcDB
	stmtId string
}

type rpcTx struct {
	*rpcDB
}

// --------  a driver implementation  -----------
type JSONLDBProxyDriver struct {
	*dbProxyDriverAdapter
}

func (d *JSONLDBProxyDriver) GetDriverName() string {
	return driver_name
}

func (d *JSONLDBProxyDriver) CreateConnection(dsn string) (dbInst interface{}, err error) {
	// fmt.Fprintf(os.Stderr, "CreateConnection(%s) called\n", dsn)
	dbProxyClient, e := jsonlrpc.Dial("tcp", jsonlProxyHost)
	if e != nil {
		err = e
		return
	}
	reconnect := jsonlrpc.Reconnect(&dbProxyClient, 3 , "tcp", jsonlProxyHost)
	c := &rpcDB{
		dbProxyClient: &dbProxyClient,
		// dbId: res.DbId,
		reconnect: reconnect,
	}

	d.dbProxyDriverAdapter.dbProxyCaller = newJSONLDBProxyCaller(c)
	var dbId string
	if dbId, err = d.openDB(dsn); err != nil {
		dbProxyClient.Close()
		return
	}
	c.dbId = dbId
	dbInst = c

	return
}

func (d *JSONLDBProxyDriver) CloseConnection(dbInst interface{}) (err error) {
	c := dbInst.(*rpcDB)
	err = d.closeDB(c.dbId)
	(*c.dbProxyClient).Close()
	return
}

func (d *JSONLDBProxyDriver) Ping(dbInst interface{}) (err error) {
	c := dbInst.(*rpcDB)
	return d.ping(c.dbId, c.txId)
}

func (d *JSONLDBProxyDriver) BeginTx(dbInst interface{}, opts driver.TxOptions) (tx interface{}, err error) {
	// fmt.Fprintf(os.Stderr, "BeginTx called\n")
	c := dbInst.(*rpcDB)
	if len(c.txId) > 0 {
		err = fmt.Errorf("only 1 transaction allowed")
		return
	}
	var txId string
	if txId, err = d.beginTx(c.dbId, opts); err != nil {
		return
	}
	c.txId = txId
	tx = &rpcTx{
		rpcDB: c,
	}
	return
}

func (d *JSONLDBProxyDriver) Commit(tx interface{}) (err error) {
	// fmt.Fprintf(os.Stderr, "Commit called\n")
	t := tx.(*rpcTx)
	err = d.commit(t.txId)
	t.rpcDB.txId = ""
	return
}

func (d *JSONLDBProxyDriver) Rollback(tx interface{}) (err error) {
	// fmt.Fprintf(os.Stderr, "Rollback called\n")
	t := tx.(*rpcTx)
	err = d.rollback(t.txId)
	t.rpcDB.txId = ""
	return
}

func (d *JSONLDBProxyDriver) Prepare(dbInst interface{}, query string) (stmt interface{}, err error) {
	// fmt.Fprintf(os.Stderr, "Prepare %s\n", query)
	c := dbInst.(*rpcDB)
	var stmtId string
	if stmtId, err = d.prepare(c.dbId, c.txId, query); err != nil {
		return
	}
	stmt = &rpcStmt {
		rpcDB: c,
		stmtId: stmtId,
	}
	return
}

func (d *JSONLDBProxyDriver) CloseStmt(stmt interface{}) (err error) {
	s := stmt.(*rpcStmt)
	// fmt.Fprintf(os.Stderr, "close stmt(%v) called\n", s.stmtId)
	return d.closeStmt(s.stmtId)
}

func (d *JSONLDBProxyDriver) Exec(stmt interface{}, args ...interface{}) (ec sd.ExecResult, err error) {
	s := stmt.(*rpcStmt)
	// fmt.Fprintf(os.Stderr, "exec stmt(%v) called\n", s.stmtId)
	return d.exec(s.stmtId, args...)
}

func (d *JSONLDBProxyDriver) Query(stmt interface{}, args ...interface{}) (r sd.ResultSet, err error) {
	s := stmt.(*rpcStmt)
	// fmt.Fprintf(os.Stderr, "query stmt(%v) called\n", s.stmtId)
	return d.query(s.stmtId, args...)
}

