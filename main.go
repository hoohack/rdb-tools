package main

import (
	"errors"
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

const RDB_6BITLEN = 0
const RDB_14BITLEN = 1
const RDB_32BITLEN = 0x80
const RDB_64BITLEN = 0x81
const RDB_ENCVAL = 3

const RDB_ENC_INT8 = 0  /* 8 bit signed integer */
const RDB_ENC_INT16 = 1 /* 16 bit signed integer */
const RDB_ENC_INT32 = 2 /* 32 bit signed integer */
const RDB_ENC_LZF = 3   /* string compressed with FASTLZ */

type Rdb struct {
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

func (r *Rdb) ReadStr(fp *os.File, length int64) (string, error) {
	buf := make([]byte, length)
	size, err := fp.ReadAt(buf[:length], r.curIndex)
	checkErr(err)

	if size < 0 {
		fmt.Fprintf(os.Stderr, "cat: error reading: %s\n", err.Error())
		return "", err
	} else {
		r.curIndex += length
		return string(buf[:]), nil
	}
}

func (r *Rdb) LoadInteger(fp *os.File, encType int) (string, error) {
	str, err := r.ReadStr(fp, 1)
	if err != nil {
		return "", err
	}

	intVal := int(str[0])

	return strconv.Itoa(intVal), nil
}

func (r *Rdb) LoadType(fp *os.File) (byte, error) {
	str, err := r.ReadStr(fp, 1)
	if err != nil {
		return 0, err
	}

	return str[0], err
}

func (r *Rdb) LoadStrLen(fp *os.File, isEncoded *bool) (int, error) {
	if isEncoded != nil {
		*isEncoded = false
	}
	lenBuf, err := r.ReadStr(fp, 1)
	if len(lenBuf) == 0 || err != nil {
		return -1, err
	}

	lenType := (lenBuf[0] & 0xC0) >> 6
	if lenType == RDB_ENCVAL {
		if isEncoded != nil {
			*isEncoded = true
		}
		return int(lenBuf[0]) & 0x3F, nil
	} else if lenType == RDB_6BITLEN {
		return int(lenBuf[0]) & 0x3F, nil
	} else {
		fmt.Printf("Unknown length encoding %d in rdbLoadLen()", lenType)
		return -1, errors.New("Unknown length encoding")
	}

	return 0, nil
}

func (r *Rdb) LoadStringObject(fp *os.File) (string, error) {
	isEncoded := false

	strLen, err := r.LoadStrLen(fp, &isEncoded)
	if err != nil {
		return "", err
	}

	if isEncoded {
		switch strLen {
		case RDB_ENC_INT8, RDB_ENC_INT16, RDB_ENC_INT32:
			return r.LoadInteger(fp, strLen)
		default:
			fmt.Println("default***********************")
			return "", errors.New("Unknown RDB string encoding type")
		}
	}

	str, err := r.ReadStr(fp, int64(strLen))
	if err != nil {
		return "", err
	}

	return str, nil
}

func main() {
	file, err := os.Open("dump.rdb")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()
	rdb := &Rdb{int64(0), 0}

	// check redis rdb file signature
	str, _ := rdb.ReadStr(file, int64(9))
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
	rdb.version = version

	fmt.Printf("Current rdb file redis version is  %d\n", version)

	for {
		// load type
		redisType, err := rdb.LoadType(file)
		checkErr(err)

		if redisType == RDB_OPCODE_AUX {
			auxKey, err := rdb.LoadStringObject(file)
			checkErr(err)

			auxVal, err := rdb.LoadStringObject(file)
			checkErr(err)
			fmt.Printf("key %s : val %s\n", auxKey, auxVal)
		} else {
			fmt.Printf("Wrong type %d\n", redisType)
			os.Exit(-1)
		}
	}
}
