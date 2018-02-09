package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"unsafe"

	json "github.com/json-iterator/go"
)

// 处理id
// head output/20170501 | awk -F"\t"  'OFS="\t"{gsub("id", "",$1);$1=$1;print $0}'
var (
	file       string
	outDir     string
	tagMap     = make(map[string]string)
	lock       sync.Mutex
	index      = 4
	emptyjs    = []byte("{}")
	modelFile  = "col.model"
	tabBs      = []byte("\t")
	keys       []string
	valueTypes []int
)

const (
	stringType = 1
	intType    = 2
)

func init() {
	flag.StringVar(&file, "file", "", "file to load")
	flag.StringVar(&outDir, "out", "/data/yiguan/output", "outdir path")
}

func main() {
	flag.Parse()
	readModel()
	process(file)
}

func process(f string) {
	r, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	info, _ := r.Stat()
	sc := bufio.NewScanner(r)
	of, _ := os.OpenFile(filepath.Join(outDir, info.Name()), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0660)
	output := bufio.NewWriter(of)

	var i = 0
	for sc.Scan() {
		i++
		bs := bytes.Split(sc.Bytes(), tabBs)
		//先输出 0-3

		output.Write(bs[0])
		output.WriteRune('\t')

		output.Write(bs[1])
		output.WriteRune('\t')

		output.Write(bs[2])
		output.WriteRune('\t')

		output.Write(bs[3])
		output.WriteRune('\t')

		it := make(map[string]interface{})
		if !bytes.Equal(bs[4], emptyjs) {
			err := json.Unmarshal(bs[4], &it)
			if err != nil {
				panic(err)
			}
		}
		for i, k := range keys {
			v := valueTypes[i]
			if v == stringType {
				if _, ok := it[k]; !ok {
					output.WriteRune(' ')
					output.WriteRune('\t')
					continue
				}
				output.WriteString(it[k].(string))
			} else {
				if _, ok := it[k]; !ok {
					output.WriteRune('0')
					output.WriteRune('\t')
					continue
				}
				output.WriteString(fmt.Sprintf("%v", it[k]))
			}
			output.WriteRune('\t')
		}

		//5 转date类型 20160707
		dd := bs[5]

		output.Write(dd[:4])
		output.WriteRune('-')
		output.Write(dd[4:6])
		output.WriteRune('-')
		output.Write(dd[6:8])
		output.WriteRune('\n')
		if i%1000000 == 0 {
			output.Flush()
		}
	}
	output.Flush()

	fmt.Println("done => ", f)
}

func readModel() {
	f, _ := os.Open(modelFile)
	err := json.NewDecoder(f).Decode(&tagMap)
	if err != nil {
		panic(err)
	}
	for k, _ := range tagMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if tagMap[k] == "String" {
			valueTypes = append(valueTypes, stringType)
		} else {
			valueTypes = append(valueTypes, intType)
		}
	}
}

func String2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func cout(b []byte) {
	fmt.Print(Bytes2String(b))
}
