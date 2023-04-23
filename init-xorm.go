//go:build xorm

package dbproxy

import (
    "xorm.io/core"
	"fmt"
	"strings"
)

func init() {
	e1 := initJSONLRpcDriver()
	if e1 == nil {
		core.RegisterDriver(driver_name, jsonlDriver)
		return
	}
	if e2 := initHttpDriver(); e2 != nil {
		panic(fmt.Sprintf("%s or %s", e1.Error(), e2.Error()))
	}
	core.RegisterDriver(driver_name, httpDriver)
}

// Parse() defined in xorm.Driver
func (d *HttpDBProxyDriver) Parse(_ string, dsn string) (*core.Uri, error) {
    return parse(dsn)
}

// Parse() defined in xorm.Driver
func (d *JSONLDBProxyDriver) Parse(_ string, dsn string) (*core.Uri, error) {
    return parse(dsn)
}

func parse(dsn string) (*core.Uri, error) {
	pos := strings.IndexByte(dsn, ':')
	if pos <= 0 {
		return nil, fmt.Errorf("unknown dsn")
	}
	return &core.Uri{DbType: core.DbType(dsn[:pos])}, nil
}
