package dbproxy

import (
	"github.com/rosbit/gnet"
	"fmt"
	"net/http"
	"encoding/json"
)

type DBProxyResult interface {
	GetCode() int
	GetMsg() string
}

type BaseResult struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}
func (b *BaseResult) GetCode() int {
	return b.Code
}
func (b *BaseResult) GetMsg() string {
	return b.Msg
}

type httpDBProxyCaller struct {
}
func newHttpDBProxyCaller() *httpDBProxyCaller {
	return &httpDBProxyCaller{}
}
func (c *httpDBProxyCaller) callDBProxy(action string, args map[string]interface{}, res interface{}) (status int, jsonl <-chan []interface{}, err error) {
	url := fmt.Sprintf("%s/%s", baseURL, action)
	var resp *http.Response
	if status, _, resp, err = gnet.JSON(url, gnet.Params(args), gnet.DontReadRespBody()); err != nil {
		return
	}
	if resp.Body == nil {
		err = fmt.Errorf("no body")
		return
	}
	j := json.NewDecoder(resp.Body)
	var baseRes struct {
		BaseResult
		Result json.RawMessage `json:"result"`
	}
	if err = j.Decode(&baseRes); err != nil {
		return
	}
	if status != http.StatusOK {
		err = fmt.Errorf("%s", baseRes.GetMsg())
		return
	}
	if len(baseRes.Result) == 0 || res == nil {
		return
	}
	if err = json.Unmarshal(baseRes.Result, res); err != nil {
		return
	}

	out := make(chan []interface{})
	go func() {
		for j.More() {
			var jl []interface{}
			if e := j.Decode(&jl); e == nil {
				out <- jl
			}
		}
		close(out)
		resp.Body.Close()
	}()

	jsonl = out
	return
}
