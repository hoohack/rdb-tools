package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const REDIS_VERSION = 8

const RDB_OPCODE_AUX = 250
const RDB_OPCODE_RESIZEDB = 251
const RDB_OPCODE_EXPIRETIME_MS = 252
const RDB_OPCODE_EXPIRETIME = 253
const RDB_OPCODE_SELECTDB = 254
const RDB_OPCODE_EOF = 255

type rdb struct {
	curIndex int64
	version  int
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func eofErr() {
	fmt.Println("Unexpected EOF reading RDB file")
	os.Exit(-1)
}

func ReadStr(fp *os.File, beginIndex *int64, length int64) (string, error) {
	buf := make([]byte, length)
	size, err := fp.ReadAt(buf[:length], *beginIndex)
	checkErr(err)

	if size < 0 {
		fmt.Fprintf(os.Stderr, "cat: error reading: %s\n", err.Error())
		return "", err
	} else {
		*beginIndex += length
		return string(buf[:]), nil
	}
}

func LoadType(fp *os.File, beginIndex *int64) (byte, error) {
	str, err := ReadStr(fp, beginIndex, 1)
	if err != nil {
		return 0, err
	}

	fmt.Println(str[0])
	return str[0], err
}

func main() {
	file, err := os.Open("dump.rdb")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()

	curIndex := int64(0)

	// check redis rdb file signature
	str, _ := ReadStr(file, &curIndex, int64(9))
	if strings.Compare("REDIS", str[0:5]) != 0 {
		fmt.Println("Wrong signature file")
		os.Exit(-1)
	}

	// check redis rdb file version
	version, err := strconv.Atoi(str[5:])
	checkErr(err)
	if version < 1 || version > REDIS_VERSION {
		fmt.Printf("Can't handle RDB format version %s\n", version)
		os.Exit(-1)
	}

	for {
		// load type
		redisType, err := LoadType(file, &curIndex)
		checkErr(err)

		if redisType == RDB_OPCODE_AUX {
			fmt.Println("parsing aux...")
		} else {
			fmt.Println(redisType)
			os.Exit(-1)
		}
	}
}
