package main

import (
	"encoding/binary"
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

const RDB_TYPE_STRING = 0
const RDB_TYPE_LIST = 1
const RDB_TYPE_SET = 2
const RDB_TYPE_ZSET = 3
const RDB_TYPE_HASH = 4
const RDB_TYPE_ZSET_2 = 5 /* ZSET version 2 with doubles stored in binary. */
const RDB_TYPE_MODULE = 6
const RDB_TYPE_MODULE_2 = 7

type redisObject struct {
}

type Rdb struct {
	curIndex    int64
	version     int
	dbId        int
	dbSize      int
	expiresSize int
	expireTime  int64
	fp          *os.File
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

func (r *Rdb) ReadStr(length int64) (string, error) {
	buf := make([]byte, length)
	size, err := r.fp.ReadAt(buf[:length], r.curIndex)
	checkErr(err)

	if size < 0 {
		fmt.Fprintf(os.Stderr, "cat: error reading: %s\n", err.Error())
		return "", err
	} else {
		r.curIndex += length
		return string(buf[:]), nil
	}
}

func (r *Rdb) LoadInteger(encType int) (string, error) {
	intVal := 0

	if encType == RDB_ENC_INT8 {
		buf, err := r.ReadStr(1)
		if err != nil {
			return "", err
		}

		intVal = int(buf[0])
	} else if encType == RDB_ENC_INT16 {
		buf, err := r.ReadStr(2)
		if err != nil {
			return "", err
		}

		intVal = int(buf[0]) | (int(buf[1]) << 8)
	} else if encType == RDB_ENC_INT32 {
		buf, err := r.ReadStr(4)
		if err != nil {
			return "", err
		}

		intVal = int(buf[0]) | (int(buf[1]) << 8) | (int(buf[2]) << 16) | (int(buf[3]) << 24)
	} else {
		intVal = 0
		return "", fmt.Errorf("Unknown RDB integer encoding type %d", encType)
	}

	return strconv.Itoa(intVal), nil
}

func (r *Rdb) LoadType() (byte, error) {
	str, err := r.ReadStr(1)
	if err != nil {
		return 0, err
	}

	return str[0], err
}

func (r *Rdb) LoadStrLen(isEncoded *bool) (int, error) {
	if isEncoded != nil {
		*isEncoded = false
	}
	lenBuf, err := r.ReadStr(1)
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
	} else if lenType == RDB_14BITLEN {
		buf, err := r.ReadStr(1)
		if err != nil {
			return 0, err
		}
		return (int(lenBuf[0])&0x3F)<<8 | int(buf[0]), nil
	} else {
		fmt.Printf("Unknown length encoding %d in rdbLoadLen()\n", lenType)
		return -1, errors.New("Unknown length encoding")
	}

	return 0, nil
}

func (r *Rdb) LoadStringObject() (string, error) {
	isEncoded := false

	strLen, err := r.LoadStrLen(&isEncoded)
	if err != nil {
		return "", err
	}

	if isEncoded {
		switch strLen {
		case RDB_ENC_INT8, RDB_ENC_INT16, RDB_ENC_INT32:
			return r.LoadInteger(strLen)
		default:
			fmt.Println("default***********************")
			return "", fmt.Errorf("Unknown RDB string encoding type: %s", strLen)
		}
	}

	str, err := r.ReadStr(int64(strLen))
	if err != nil {
		return "", err
	}

	return str, nil
}

func (r *Rdb) LoadMillisecondTime() (int64, error) {
	buf, err := r.ReadStr(8)
	if err != nil {
		return 0, err
	}

	bufByte := []byte(buf)
	expireTime := int64(binary.LittleEndian.Uint64(bufByte))

	return expireTime, nil
}

func (r *Rdb) LoadObject(loadType byte) (string, error) {
	if loadType == RDB_TYPE_STRING {
		strVal, err := r.LoadStringObject()
		if err != nil {
			fmt.Println("Fail to load string object")
			os.Exit(-1)
		}

		return strVal, nil
	} else {
		return "", nil
	}
}

func main() {
	file, err := os.Open("dump-8003.rdb")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()
	rdb := &Rdb{int64(0), 0, 0, 0, 0, 0, file}

	// check redis rdb file signature
	str, _ := rdb.ReadStr(int64(9))
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

	fmt.Printf("Rdb file version: %d\n", version)

	for {
		// load type
		redisType, err := rdb.LoadType()
		checkErr(err)

		if redisType == RDB_OPCODE_AUX {
			auxKey, err := rdb.LoadStringObject()
			checkErr(err)

			auxVal, err := rdb.LoadStringObject()
			checkErr(err)
			fmt.Printf("%s: %s\n", auxKey, auxVal)

			continue
		} else if redisType == RDB_OPCODE_SELECTDB {
			dbId, err := rdb.LoadStrLen(nil)
			if err != nil {
				fmt.Println("Fail to load dbId")
				os.Exit(-1)
			}

			rdb.dbId = dbId
			fmt.Printf("Selected DB: %d\n", rdb.dbId)

			continue
		} else if redisType == RDB_OPCODE_RESIZEDB {
			dbSize, err := rdb.LoadStrLen(nil)
			if err != nil {
				fmt.Println("Fail to load dbSize")
				os.Exit(-1)
			}

			expiresSize, err := rdb.LoadStrLen(nil)
			if err != nil {
				fmt.Println("Fail to load expires size")
				os.Exit(-1)
			}

			rdb.dbSize = dbSize
			rdb.expiresSize = expiresSize

			fmt.Printf("Rdb dbSize: %d\n", rdb.dbSize)
			fmt.Printf("Rdb expiresSize: %d\n", rdb.expiresSize)

			continue
		} else if redisType == RDB_OPCODE_EXPIRETIME_MS {
			rdb.expireTime, err = rdb.LoadMillisecondTime()
			if err != nil {
				fmt.Println("Fail to load millisecondtime")
				os.Exit(-1)
			}

			redisType, err = rdb.LoadType()
			checkErr(err)
		} else if redisType == RDB_OPCODE_EOF {
			fmt.Println("Reach file eof, parsing work finished")
			break
		}

		redisKey, err := rdb.LoadStringObject()
		checkErr(err)

		redisVal, err := rdb.LoadObject(redisType)
		checkErr(err)

		fmt.Printf("%s: %s\n", redisKey, redisVal)
	}
}
