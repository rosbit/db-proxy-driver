package dbproxy

import (
	"github.com/rosbit/jsonl-rpc"
	"fmt"
	"net/rpc"
	"net/http"
	"encoding/json"
)

type dbProxyRequest struct {
    Action string      `json:"action"`
    Args   interface{} `json:"args"`
}

const doDBProxy = "DBProxy.Do"

type jsonlDBProxyCaller struct {
	*rpcDB
	doQuerying bool
}
func newJSONLDBProxyCaller(dbInst *rpcDB, doQuerying ...bool) *jsonlDBProxyCaller {
	if len(doQuerying) > 0 && doQuerying[0] {
		return &jsonlDBProxyCaller{dbInst, doQuerying[0]}
	}
	return &jsonlDBProxyCaller{rpcDB: dbInst}
}

func (c *jsonlDBProxyCaller) callDBProxy(action string, args map[string]interface{}, res interface{}) (status int, jsonl <-chan []interface{}, err error) {
	req := &dbProxyRequest {
		Action: action,
		Args: args,
	}

	if !c.doQuerying {
		// synchronized
		status, err = c.callDBProxyWithoutJSONL(req, res)
		return
	}

	// asynchronized
	status, jsonl, err = c.callDBProxyWithJSONL(req, res)
	return
}

func (c *jsonlDBProxyCaller) callDBProxyWithoutJSONL(req *dbProxyRequest, res interface{}) (status int, err error) {
	jsonlRes := &jsonlrpc.JSONLClientResponse{}

	if err = (*c.dbProxyClient).Call(doDBProxy, req, jsonlRes); err != nil {
		if err != rpc.ErrShutdown {
			status = http.StatusInternalServerError
			return
		}
		if err = c.reconnect(); err != nil {
			status = http.StatusInternalServerError
			return
		}
		if err = (*c.dbProxyClient).Call(doDBProxy, req, jsonlRes); err != nil {
			status = http.StatusInternalServerError
			return
		}
	}

	status = jsonlRes.Code
	if status != http.StatusOK {
		err = fmt.Errorf("%s", jsonlRes.Msg)
		return
	}
	if res == nil || len(jsonlRes.Result) == 0 {
		return
	}
	if err = json.Unmarshal(jsonlRes.Result, res); err != nil {
		return
	}
	return
}

func (c *jsonlDBProxyCaller) callDBProxyWithJSONL(req *dbProxyRequest, res interface{}) (status int, jsonl <-chan []interface{}, err error) {
	done := make(chan struct{})
	collectRows := func(respRes json.RawMessage, rows <-chan interface{}) {
		defer close(done)

		if res != nil && len(respRes) > 0 {
			if err = json.Unmarshal(respRes, res); err != nil {
				status = http.StatusInternalServerError
			} else {
				status = http.StatusOK
			}
		}

		res := make(chan []interface{})
		jsonl = res
		go func() {
			for row := range rows {
				if v, ok := row.([]interface{}); ok {
					res <- v
				}
			}
			close(res)
		}()
		return
	}
	jsonlRes := &jsonlrpc.JSONLClientResponse{
		CollectJSONLs: collectRows,
	}

	go func() {
		if err = (*c.dbProxyClient).Call(doDBProxy, req, jsonlRes); err != nil {
			status = http.StatusInternalServerError
			close(done)
			return
		}
	}()
	<-done
	return
}
