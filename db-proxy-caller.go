package dbproxy

type dbProxyCaller interface {
	callDBProxy(action string, args map[string]interface{}, res interface{}) (status int, jsonl <-chan []interface{}, err error)
}
