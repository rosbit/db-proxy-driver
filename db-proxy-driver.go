package dbproxy

import (
	sd "github.com/rosbit/go-sql-driver"
	"database/sql/driver"
)

type dbProxyDriverAdapter struct {
	dbProxyCaller
}

func (d *dbProxyDriverAdapter) openDB(dsn string) (dbId string, err error) {
	var res struct {
		DbId string `json:"db-id"`
	}
	if _, _, err = d.callDBProxy("open-db", map[string]interface{}{"dsn": dsn}, &res); err != nil {
		return
	}
	dbId = res.DbId
	return
}

func (d *dbProxyDriverAdapter) closeDB(dbId string) (err error) {
	_, _, err = d.callDBProxy("close-db", map[string]interface{}{"did": dbId}, nil)
	return
}

func (d *dbProxyDriverAdapter) ping(dbId string, txId string) (err error) {
	_, _, err = d.callDBProxy("ping", map[string]interface{}{
		"did": dbId,
		"tid": txId,
	}, nil)
	return
}

func (d *dbProxyDriverAdapter) beginTx(dbId string, opts driver.TxOptions) (txId string, err error) {
	var res struct {
		TxId string `json:"tx-id"`
	}
	if _, _, err = d.callDBProxy("begin-tx", map[string]interface{}{
		"did": dbId,
		"opts": opts,
	}, &res); err != nil {
		return
	}
	txId = res.TxId
	return
}

func (d *dbProxyDriverAdapter) commit(txId string) (err error) {
	_, _, err = d.callDBProxy("commit", map[string]interface{}{"tid": txId}, nil)
	return
}

func (d *dbProxyDriverAdapter) rollback(txId string) (err error) {
	_, _, err = d.callDBProxy("rollback", map[string]interface{}{"tid": txId}, nil)
	return
}

func (d *dbProxyDriverAdapter) prepare(dbId string, txId string, query string) (stmtId string, err error) {
	var res struct {
		StmtId string `json:"stmt-id"`
	}
	if _, _, err = d.callDBProxy("prepare", map[string]interface{}{
		"did": dbId,
		"query": query,
		"tid": txId,
	}, &res); err != nil {
		return
	}
	stmtId = res.StmtId
	return
}

func (d *dbProxyDriverAdapter) closeStmt(stmtId string) (err error) {
	_, _, err = d.callDBProxy("close-stmt", map[string]interface{}{"sid": stmtId}, nil)
	return
}

func (d *dbProxyDriverAdapter) exec(stmtId string, args ...interface{}) (ec sd.ExecResult, err error) {
	var res struct {
		LastInsertId int64 `json:"lastInsertId"`
		RowsAffected int64 `json:"rowsAffected"`
	}
	if _, _, err = d.callDBProxy("exec", map[string]interface{}{
		"sid": stmtId,
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

func (d *dbProxyDriverAdapter) query(stmtId string, args ...interface{}) (r sd.ResultSet, err error) {
	var res struct {
		Columns []string `json:"columns"`
	}
	_, jsonl, e := d.callDBProxy("query", map[string]interface{}{
		"sid": stmtId,
		"args": args,
	}, &res, true);
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
