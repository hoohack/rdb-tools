package main

/*
 * store redis object
 * type int
 * val  interface
 */
type RedisObject struct {
	objType int
	objLen  int
	objVal  interface{}
}

func NewRedisObject(objType int, objVal interface{}) *RedisObject {
	return &RedisObject{objType, objVal}
}
