# db-proxy-driver

`db-proxy-driver` is an implementation of database/sql/driver to access [db-proxy](https://github.com/rosbit/db-proxy),
which is a proxy database server to any real database servers.

## Usage

1. register driver
```go
import (
    _  "github.com/rosbit/db-proxy-driver"
)
```

2. driver name and DSN
   - the driver name is `"dbproxy"`
   - the format of DSN depends on the real driver & DSN. e.go., the origininal DSN for driver `mysql` is
     named `"user:password@tcp(host:port)/db?charset=utf8mb4"`, the new DSN is "mysql:user:password@tcp(host:port)db?charset=utf8mb4",
     which is the concatation of the real driver name, the colon and the orgininal DSN.

3. specify the `db-proxy` server
    If the `db-proxy` server is listening at host:http-port for HTTP and WebSocket service,
    host:rpc-port for net/rpc service, environment is used to specfiy where is the `db-proxy` server, and
    which protocol is used.
   - HTTP_DB_PROXY_BASEURL=http://host:http-port will make `db-proxy-driver` to access `db-proxy` with HTTP
   - JSONLRPC_DB_PROXY_HOST=host:rpc-port will make `db-proxy-driver` to access `db-proxy` with net/rpc
   - JSONLRPC_DB_PROXY_WS_HOST=host:http-port will make `db-proxy-driver` to access `db-proxy` with net/rpc over Websocket

4. using ORM package `xorm`
   - `xorm` is supported, just add a `go build` tag like the following.
     `go build -tags xorm`

