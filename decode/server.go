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
const KeyNotExists = 1000

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

type RetData struct {
	Type     int         `json:"type"`
	TypeName string      `json:"typeName"`
	Val      interface{} `json:"val"`
}

/*
* API 返回结果数据结构定义
 */
type ReturnResult struct {
	Code   int         `json:"code"`
	ErrMsg string      `json:"errMsg"`
	Data   interface{} `json:"data"`
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

	result := &ReturnResult{Success, "", retArr}
	response, err := json.MarshalIndent(result, "", " ")
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(w, string(response))
}

/*
* 获取某个key
* @param key
 */
func (rh *RdbHandler) getKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	keyVar, ok := vars["key"]
	if !ok {

	}

	var result *ReturnResult
	rdb := rh.rdb
	ret, ok := rdb.mapObj[keyVar]
	if ok {
		typeMap := map[int]string{
			0: "string",
			1: "list",
			2: "set",
			3: "zset",
			4: "hash",
			5: "zset",
		}
		retData := &RetData{ret.objType, typeMap[ret.objType], ret.objVal}
		result = &ReturnResult{Success, "", retData}
	} else {
		result = &ReturnResult{KeyNotExists, fmt.Sprintf("key %s not exists", keyVar), nil}
	}

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
	router.HandleFunc("/key/{key}", rh.getKey)

	// 静态资源路由
	router.Handle("/", http.FileServer(http.Dir("./www")))
	router.Handle("/css/{rest}", http.StripPrefix("/css/", http.FileServer(http.Dir("./www/css/"))))
	router.Handle("/js/{rest}", http.StripPrefix("/js/", http.FileServer(http.Dir("./www/js/"))))

	// 启动服务，监听请求
	err = http.ListenAndServe(":5763", router)
	if err != nil {
		fmt.Printf("start server failed, errmsg: %s\n", err)
	}
}
