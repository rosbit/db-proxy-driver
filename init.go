package dbproxy

import (
	"fmt"
)

func init() {
	e1 := initJSONLRpcDriver()
	if e1 == nil {
		return
	}
	if e2 := initHttpDriver(); e2 != nil {
		panic(fmt.Sprintf("%s or %s", e1.Error(), e2.Error()))
	}
}
