package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
)

const PageSize = 1
const Success = 0

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

func (rh *rdbHandler) MakeReturnData(code int, errMsg string, data interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	result["code"] = code
	result["errMsg"] = errMsg
	result["data"] = data

	return result
}

/*
* 获取所有的key列表
* @param page int
* @return []byte
 */
func (rh *rdbHandler) getAllKeys(page int) []string {
	var keysArr []string
	rdb := rh.rdb
	for k, _ := range rdb.mapObj {
		keysArr = append(keysArr, k)
	}

	sort.Strings(keysArr)

	var retArr []string
	if page == 0 {
		retArr = keysArr
	}

	offset := ((page - 1) * PageSize)
	if offset < len(keysArr) {
		nextPos := offset + PageSize
		if nextPos > len(keysArr) {
			nextPos = len(keysArr)
		}
		retArr = keysArr[offset:nextPos]
	}

	return retArr
}

func (rh *rdbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	urlArr := strings.Split(r.URL.Path, "/")
	apiName := urlArr[1]
	switch apiName {
	case "keys":
		var page int = 1
		if len(urlArr) > 2 {
			page, err := strconv.Atoi(urlArr[1])
			if err != nil {
				fmt.Printf("convert string to int failed, page: %s", page)
				return
			}
		}
		arrKeys := rh.getAllKeys(page)
		result := rh.MakeReturnData(Success, "", arrKeys)
		response, err := json.MarshalIndent(result, "", " ")
		if err != nil {
			panic(err)
		}

		fmt.Fprintf(w, string(response))
		break
	}
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
	http.Handle("/keys", &rdbHandler{rdb})

	err = http.ListenAndServe(":5763", nil)
	if err != nil {
		fmt.Printf("start server failed, errmsg: %s\n", err)
	}
}
