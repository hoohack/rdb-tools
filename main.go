package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
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

const RDB_TYPE_HASH_ZIPMAP = 9
const RDB_TYPE_LIST_ZIPLIST = 10
const RDB_TYPE_SET_INTSET = 11
const RDB_TYPE_ZSET_ZIPLIST = 12
const RDB_TYPE_HASH_ZIPLIST = 13
const RDB_TYPE_LIST_QUICKLIST = 14

/* 字符串编码类型定义 */
const ZIP_STR_06B = 0
const ZIP_STR_14B = 1
const ZIP_STR_32B = 2

/* 整数编码类型定义 */
const ZIP_INT_16B = 0xC0
const ZIP_INT_32B = 0xD0
const ZIP_INT_64B = 0xE0
const ZIP_INT_24B = 0xF0
const ZIP_INT_8B = 0xfe
const ZIP_INT_4B = 15

type Rdb struct {
	curIndex    int64
	version     int
	dbId        int
	dbSize      int
	expiresSize int
	expireTime  int64
	fp          *os.File
	rdbType     int
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

func (r *Rdb) ReadBuf(length int64) ([]byte, error) {
	buf := make([]byte, length)
	size, err := r.fp.ReadAt(buf[:length], r.curIndex)
	checkErr(err)

	if size < 0 {
		fmt.Fprintf(os.Stderr, "cat: error reading: %s\n", err.Error())
		return []byte{}, err
	} else {
		r.curIndex += length
		return buf, nil
	}
}

func (r *Rdb) LoadInteger(encType int) (string, error) {
	intVal := 0

	if encType == RDB_ENC_INT8 {
		buf, err := r.ReadBuf(1)
		if err != nil {
			return "", err
		}

		intVal = int(buf[0])
	} else if encType == RDB_ENC_INT16 {
		buf, err := r.ReadBuf(2)
		if err != nil {
			return "", err
		}

		intVal = int(buf[0]) | (int(buf[1]) << 8)
	} else if encType == RDB_ENC_INT32 {
		buf, err := r.ReadBuf(4)
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

func (r *Rdb) lzfDecompress(compressedBuf []byte, inLen int, strLen int) string {
	decompressedRet := make([]byte, strLen)
	for i, j := 0, 0; i < inLen; {
		ctrl := int(compressedBuf[i])
		i++
		if ctrl < (1 << 5) {
			for x := 0; x <= ctrl; x++ {
				decompressedRet[j] = compressedBuf[i]
				i++
				j++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(compressedBuf[i])
				i++
			}
			ref := j - ((ctrl & 0x1f) << 8) - int(compressedBuf[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				decompressedRet[j] = decompressedRet[ref]
				ref++
				j++
			}
		}
	}

	return string(decompressedRet)
}

func (r *Rdb) LoadLzfString(encType int) (string, error) {
	cLen, err := r.LoadLen(nil)
	if err != nil {
		fmt.Println("Fail to load len")
	}

	sLen, err := r.LoadLen(nil)
	if err != nil {
		fmt.Println("Fail to load len")
	}

	compressedBuf, err := r.ReadBuf(int64(cLen))
	if err != nil {
		fmt.Println(compressedBuf)
		return "", err
	}

	deCompressedStr := r.lzfDecompress(compressedBuf, cLen, sLen)

	return deCompressedStr, nil
}

func (r *Rdb) LoadType() (byte, error) {
	str, err := r.ReadBuf(1)
	if err != nil {
		return 0, err
	}

	return str[0], err
}

func (r *Rdb) LoadLen(isEncoded *bool) (int, error) {
	if isEncoded != nil {
		*isEncoded = false
	}
	lenBuf, err := r.ReadBuf(1)
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
		buf, err := r.ReadBuf(1)
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

	strLen, err := r.LoadLen(&isEncoded)
	if err != nil {
		return "", err
	}

	if isEncoded {
		switch strLen {
		case RDB_ENC_INT8, RDB_ENC_INT16, RDB_ENC_INT32:
			return r.LoadInteger(strLen)
		case RDB_ENC_LZF:
			return r.LoadLzfString(strLen)
		default:
			return "", fmt.Errorf("Unknown RDB string encoding type: %s", strLen)
		}
	}

	buf, err := r.ReadBuf(int64(strLen))
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (r *Rdb) LoadMillisecondTime() (int64, error) {
	buf, err := r.ReadBuf(8)
	if err != nil {
		return 0, err
	}

	bufByte := []byte(buf)
	expireTime := int64(binary.LittleEndian.Uint64(bufByte))

	return expireTime, nil
}

func (r *Rdb) LoadZSetSize(setBuf string) (int64, error) {
	bufByte := []byte(setBuf[8:10])

	return int64(binary.LittleEndian.Uint16(bufByte)), nil
}

/*
* ziplist format
* <length-prev-entry><special-flag><raw-bytes-of-entry>
*       zlbytes: a 4 byte unsigned integer representing the total size in bytes of the ziplist. The 4 bytes are in little endian format - the least significant bit comes first.
*       zltail: a 4 byte unsigned integer in little endian format. It represents the offset to the tail (i.e. last) entry in the ziplist
*       zllen: This is a 2 byte unsigned integer in little endian format. It represents the number of entries in this ziplist
*       entry: An entry represents an element in the ziplist. Details below
*       zlend: Always 255. It represents the end of the ziplist.
*
* Each entry in the ziplist has the following format :
*        <length-prev-entry><special-flag><raw-bytes-of-entry>
* length-prev-entry: stores the length of the previous entry, or 0 if this is the first entry. This allows easy traversal of the list in the reverse direction. This length is stored in either 1 byte or in 5 bytes. If the first byte is less than or equal to 253, it is considered as the length. If the first byte is 254, then the next 4 bytes are used to store the length. The 4 bytes are read as an unsigned integer.
* special-flag: This flag indicates whether the entry is a string or an integer. It also indicates the length of the string, or the size of the integer. The various encodings of this flag are shown below:
* Bytes                 Length  Meaning
* 00pppppp              1 byte  String value with length less than or equal to 63 bytes (6 bits)
* 01pppppp|qqqqqqqq     2 bytes String value with length less than or equal to 16383 bytes (14 bits)
* 10______|<4 byte>     5 bytes Next 4 byte contain an unsigned int. String value with length greater than or equal to 16384 bytes
* 1100____              3 bytes Integer encoded as 16 bit signed (2 bytes)
* 1101____              5 bytes Integer encoded as 32 bit signed (4 bytes)
* 1110____              9 bytes Integer encoded as 64 bit signed (8 bytes)
* 1111____              4 bytes Integer encoded as 24 bit signed (3 bytes)
 */
func (r *Rdb) LoadZipListEntry(setBuf string, curIndex *int) (string, error) {
	prevEntryLen := byte(setBuf[*curIndex])
	*curIndex++

	if prevEntryLen == 254 {
		*curIndex += 4
	}

	specialFlag := byte(setBuf[*curIndex])

	*curIndex++
	switch {
	case specialFlag>>6 == ZIP_STR_06B:
		strLen := int(specialFlag & 0x3f)

		nextIndex := *curIndex + strLen
		valBuf := setBuf[*curIndex:nextIndex]

		*curIndex = nextIndex

		return valBuf, nil
	case specialFlag>>6 == ZIP_STR_14B:
		nextIndex := *curIndex + 1
		valBuf := byte(setBuf[nextIndex])
		*curIndex++

		nextIndex = (int(specialFlag&0x3f) << 8) | int(valBuf)
		*curIndex = nextIndex

		return setBuf[*curIndex:nextIndex], nil
	case specialFlag>>6 == ZIP_STR_32B:
		nextIndex := *curIndex + 4
		lenBuf := []byte(setBuf[*curIndex:nextIndex])
		*curIndex = nextIndex

		nextIndex = int(binary.BigEndian.Uint32(lenBuf))
		*curIndex = nextIndex

		return setBuf[*curIndex:nextIndex], nil
	case specialFlag == ZIP_INT_8B:
		valBuf := byte(setBuf[*curIndex])
		*curIndex++

		return strconv.FormatInt(int64(int8(valBuf)), 10), nil
	case specialFlag == ZIP_INT_16B:
		nextIndex := *curIndex + 2
		valBuf := []byte(setBuf[*curIndex:nextIndex])

		*curIndex = nextIndex

		return strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(valBuf))), 10), nil
	case specialFlag == ZIP_INT_24B:
		nextIndex := *curIndex + 3
		valBuf := []byte(setBuf[*curIndex:nextIndex])

		// a trick to do array unshift
		valBuf = append([]byte{0}, valBuf[0], valBuf[1], valBuf[2])

		*curIndex = nextIndex

		return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(valBuf))>>8), 10), nil
	case specialFlag == ZIP_INT_32B:
		nextIndex := *curIndex + 4
		valBuf := []byte(setBuf[*curIndex:nextIndex])

		*curIndex = nextIndex

		return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(valBuf))), 10), nil
	case specialFlag == ZIP_INT_64B:
		nextIndex := *curIndex + 8
		valBuf := []byte(setBuf[*curIndex:nextIndex])

		*curIndex = nextIndex

		return strconv.FormatInt(int64(binary.LittleEndian.Uint64(valBuf)), 10), nil
	case specialFlag>>4 == ZIP_INT_4B:
		return strconv.FormatInt(int64(specialFlag&0x0f)-1, 10), nil
	}

	return "", fmt.Errorf("unknown ziplist specialFlag: %d", specialFlag)
}

func (r *Rdb) LoadObject(objType byte) (string, error) {
	if objType == RDB_TYPE_STRING {
		strVal, err := r.LoadStringObject()
		if err != nil {
			fmt.Println("Fail to load string object")
			return "", err
		}

		return strVal, nil
	} else if objType == RDB_TYPE_HASH {
		objLen, err := r.LoadLen(nil)
		if err != nil {
			fmt.Println("Fail to load hash object len")
			return "", err
		}

		i := 0
		for {
			if i >= objLen {
				break
			}
			hashField, err := r.LoadStringObject()
			if err != nil {
				fmt.Println("Fail to load hash field")
				return "", err
			}

			hashValue, err := r.LoadStringObject()
			if err != nil {
				fmt.Println("Fail to load hash value")
				return "", err
			}

			fmt.Printf("%s => %s\n", hashField, hashValue)
			i++
		}

		return "", nil
	} else if objType == RDB_TYPE_ZSET_ZIPLIST {
		r.rdbType = RDB_TYPE_ZSET_ZIPLIST
		encodedStr, err := r.LoadStringObject()
		if err != nil {
			fmt.Println("Fail to load string")
			return "", err
		}

		fmt.Printf("len: %d\n", len(encodedStr))
		setSize, err := r.LoadZSetSize(encodedStr)
		if err != nil {
			return "", err
		}

		setSize /= 2
		curIndex := 10
		decodeStr := ""

		for i := int64(0); i < setSize; i++ {
			member, err := r.LoadZipListEntry(encodedStr, &curIndex)
			if err != nil {
				return "", err
			}

			scoreBuf, err := r.LoadZipListEntry(encodedStr, &curIndex)
			if err != nil {
				return "", err
			}

			scoreVal, err := strconv.ParseFloat(scoreBuf, 64)
			if err != nil {
				return "", err
			}

			decodeStr += fmt.Sprintf("%s => %.0f ; ", member, scoreVal)
		}

		fmt.Printf("decodeStr: %s", decodeStr)

		return decodeStr, nil
	} else if objType == RDB_TYPE_HASH_ZIPLIST {
		r.rdbType = RDB_TYPE_HASH_ZIPLIST
		encodedStr, err := r.LoadStringObject()
		if err != nil {
			fmt.Println("Fail to load string")
			return "", err
		}

		hashSize, err := r.LoadZSetSize(encodedStr)
		if err != nil {
			return "", err
		}

		hashSize /= 2
		curIndex := 10
		decodeStr := ""
		for i := int64(0); i < hashSize; i++ {
			hashField, err := r.LoadZipListEntry(encodedStr, &curIndex)
			if err != nil {
				return "", err
			}

			hashValue, err := r.LoadZipListEntry(encodedStr, &curIndex)
			if err != nil {
				return "", err
			}

			decodeStr += fmt.Sprintf("%s => %s ; ", hashField, string(hashValue))
		}

		return decodeStr, nil
	} else if objType == RDB_TYPE_SET_INTSET {
		encodedStr, err := r.LoadStringObject()
		if err != nil {
			fmt.Println("Fail to load string")
			return "", err
		}

		fmt.Printf("INTSET encoded :%s\n", encodedStr)

		return "", nil
	} else {
		fmt.Printf("object type %d\n", objType)
		return "", nil
	}
}

func main() {
	file, err := os.Open("dump.rdb")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()
	rdb := &Rdb{int64(0), 0, 0, 0, 0, 0, file, 0}

	// check redis rdb file signature
	buf, _ := rdb.ReadBuf(int64(9))
	if bytes.Compare([]byte("REDIS"), buf[0:5]) != 0 {
		fmt.Println("Wrong signature file")
		os.Exit(-1)
	}

	// check redis rdb file version
	version, err := strconv.Atoi(string(buf[5:]))
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
			dbId, err := rdb.LoadLen(nil)
			if err != nil {
				fmt.Println("Fail to load dbId")
				os.Exit(-1)
			}

			rdb.dbId = dbId
			fmt.Printf("Selected DB: %d\n", rdb.dbId)

			continue
		} else if redisType == RDB_OPCODE_RESIZEDB {
			dbSize, err := rdb.LoadLen(nil)
			if err != nil {
				fmt.Println("Fail to load dbSize")
				os.Exit(-1)
			}

			expiresSize, err := rdb.LoadLen(nil)
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
		fmt.Printf("key: %s\n", redisKey)

		redisVal, err := rdb.LoadObject(redisType)
		checkErr(err)

		fmt.Printf("%s: %s\n", redisKey, redisVal)
	}
}
