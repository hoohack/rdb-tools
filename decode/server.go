package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
)

/*
* 判断路径是否存在
* @param path string
* @return bool
 */
func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil || os.IsExist(err) {
		return true
	}

	return false
}

type rdbHandler struct {
	rdb *Rdb
}

func (rh *rdbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var keysArr []string
	rdb := rh.rdb
	for k, _ := range rdb.mapObj {
		keysArr = append(keysArr, k)
	}

	fmt.Printf("rdb keys *: %s\n", strings.Join(keysArr, " "))
	fmt.Printf("url: %s\n", r.URL)
	fmt.Printf("method: %s\n", r.Method)
}

func main() {
	// 获取文件路径
	argLen := len(os.Args)
	if argLen != 2 {
		fmt.Println("Wrong params, use decode path[eg:/home/root/dump.rdb]")
		os.Exit(-1)
	}

	// 检查文件路径合法性
	rdbFile := os.Args[1]
	if !PathExists(rdbFile) {
		fmt.Errorf("File: %s not exists, please check your file path")
		os.Exit(-1)
	}

	// 开始解析文件
	file, err := os.Open(rdbFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	mapObj := make(map[string]*RedisObject)
	defer file.Close()
	rdb := &Rdb{int64(0), 0, 0, 0, 0, 0, file, 0, mapObj}
	rdb.DecodeRDBFile()

	fmt.Println("Listening on 5763...")
	// 启动服务，监听请求
	http.Handle("/", &rdbHandler{rdb})
	err = http.ListenAndServe(":5763", nil)
	if err != nil {
		fmt.Printf("start server failed, errmsg: %s\n", err)
	}
}