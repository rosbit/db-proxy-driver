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
	jsonlDriver = &JSONLDBProxyDriver{}
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

	var res struct {
		DbId string `json:"db-id"`
	}
	if _, _, err = newJSONLDBProxyCaller(c).callDBProxy("open-db", map[string]interface{}{"dsn": dsn}, &res); err != nil {
		dbProxyClient.Close()
		return
	}
	c.dbId = res.DbId
	dbInst = c

	return
}

func (d *JSONLDBProxyDriver) CloseConnection(dbInst interface{}) (err error) {
	c := dbInst.(*rpcDB)
	_, _, err = newJSONLDBProxyCaller(c).callDBProxy("close-db", map[string]interface{}{"did": c.dbId}, nil)
	(*c.dbProxyClient).Close()
	return
}

func (d *JSONLDBProxyDriver) Ping(dbInst interface{}) (err error) {
	c := dbInst.(*rpcDB)
	_, _, err = newJSONLDBProxyCaller(c).callDBProxy("ping", map[string]interface{}{
		"did": c.dbId,
		"tid": c.txId,
	}, nil)
	return
}

func (d *JSONLDBProxyDriver) BeginTx(dbInst interface{}, opts driver.TxOptions) (tx interface{}, err error) {
	// fmt.Fprintf(os.Stderr, "BeginTx called\n")
	c := dbInst.(*rpcDB)
	if len(c.txId) > 0 {
		err = fmt.Errorf("only 1 transaction allowed")
		return
	}
	var res struct {
		TxId string `json:"tx-id"`
	}
	if _, _, err = newJSONLDBProxyCaller(c).callDBProxy("begin-tx", map[string]interface{}{
		"did": c.dbId,
		"opts": opts,
	}, &res); err != nil {
		return
	}
	c.txId = res.TxId
	tx = &rpcTx{
		rpcDB: c,
	}
	return
}

func (d *JSONLDBProxyDriver) Commit(tx interface{}) (err error) {
	// fmt.Fprintf(os.Stderr, "Commit called\n")
	t := tx.(*rpcTx)
	_, _, err = newJSONLDBProxyCaller(t.rpcDB).callDBProxy("commit", map[string]interface{}{"tid": t.txId}, nil)
	t.rpcDB.txId = ""
	return
}

func (d *JSONLDBProxyDriver) Rollback(tx interface{}) (err error) {
	// fmt.Fprintf(os.Stderr, "Rollback called\n")
	t := tx.(*rpcTx)
	_, _, err = newJSONLDBProxyCaller(t.rpcDB).callDBProxy("rollback", map[string]interface{}{"tid": t.txId}, nil)
	t.rpcDB.txId = ""
	return
}

func (d *JSONLDBProxyDriver) Prepare(dbInst interface{}, query string) (stmt interface{}, err error) {
	// fmt.Fprintf(os.Stderr, "Prepare %s\n", query)
	c := dbInst.(*rpcDB)
	var res struct {
		StmtId string `json:"stmt-id"`
	}
	if _, _, err = newJSONLDBProxyCaller(c).callDBProxy("prepare", map[string]interface{}{
		"did": c.dbId,
		"query": query,
		"tid": c.txId,
	}, &res); err != nil {
		return
	}
	stmt = &rpcStmt {
		rpcDB: c,
		stmtId: res.StmtId,
	}
	return
}

func (d *JSONLDBProxyDriver) CloseStmt(stmt interface{}) (err error) {
	s := stmt.(*rpcStmt)
	// fmt.Fprintf(os.Stderr, "close stmt(%v) called\n", s.stmtId)
	_, _, err = newJSONLDBProxyCaller(s.rpcDB).callDBProxy("close-stmt", map[string]interface{}{"sid": s.stmtId}, nil)
	return
}

func (d *JSONLDBProxyDriver) Exec(stmt interface{}, as ...interface{}) (ec sd.ExecResult, err error) {
	s := stmt.(*rpcStmt)
	// fmt.Fprintf(os.Stderr, "exec stmt(%v) called\n", s.stmtId)
	var res struct {
		LastInsertId int64 `json:"lastInsertId"`
		RowsAffected int64 `json:"rowsAffected"`
	}
	if _, _, err = newJSONLDBProxyCaller(s.rpcDB).callDBProxy("exec", map[string]interface{}{
		"sid": s.stmtId,
		"args": as,
	}, &res); err != nil {
		return
	}
	ec = &Result{
		lastInsertId: res.LastInsertId,
		rowsAffected: res.RowsAffected,
	}
	return
}

func (d *JSONLDBProxyDriver) Query(stmt interface{}, as ...interface{}) (r sd.ResultSet, err error) {
	s := stmt.(*rpcStmt)
	// fmt.Fprintf(os.Stderr, "query stmt(%v) called\n", s.stmtId)
	var res struct {
		Columns []string `json:"columns"`
	}
	_, rows, e := newJSONLDBProxyCaller(s.rpcDB, true).callDBProxy("query", map[string]interface{}{
		"sid": s.stmtId,
		"args": as,
	}, &res)
	if e != nil {
		err = e
		return
	}
	r = &ResultSet{
		columns: res.Columns,
		rows: rows,
	}
	return
}

