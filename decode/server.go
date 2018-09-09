package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"

	"github.com/gorilla/mux"
)

const PageSize = 5
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

type RdbHandler struct {
	rdb *Rdb
}

/*
 * 构造返回参数
 * @param code   int
 * @param errMsg string
 * @param data   interface
 * @return map[string]interface{}
 */
func (rh *RdbHandler) MakeReturnData(code int, errMsg string, data interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	result["code"] = code
	result["errMsg"] = errMsg
	result["data"] = data

	return result
}

/*
* 获取所有的key列表
 */
func (rh *RdbHandler) getAllKeys(w http.ResponseWriter, r *http.Request) {
	var keysArr []string
	rdb := rh.rdb
	for k, _ := range rdb.mapObj {
		keysArr = append(keysArr, k)
	}

	sort.Strings(keysArr)

	var page int = 1
	vars := mux.Vars(r)
	pageVar, ok := vars["page"]
	if ok {
		page, err := strconv.Atoi(pageVar)
		if err != nil {
			fmt.Printf("convert string to int failed, page: %s", page)
			return
		}
	}
	var retArr []string

	offset := ((page - 1) * PageSize)
	if offset < len(keysArr) {
		nextPos := offset + PageSize
		if nextPos > len(keysArr) {
			nextPos = len(keysArr)
		}
		retArr = keysArr[offset:nextPos]
	}

	result := rh.MakeReturnData(Success, "", retArr)
	response, err := json.MarshalIndent(result, "", " ")
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(w, string(response))
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
	rh := &RdbHandler{rdb}

	fmt.Println("Listening on 5763...")
	// 设置路由函数规则
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/keys/{page}", rh.getAllKeys)

	// 启动服务，监听请求
	err = http.ListenAndServe(":5763", router)
	if err != nil {
		fmt.Printf("start server failed, errmsg: %s\n", err)
	}
}
