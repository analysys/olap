package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"unsafe"

	json "github.com/json-iterator/go"
)

//这个脚本处理所有文件的数据,在当前目录动态生成 modelFile 文件
var (
	files  string
	tagMap = make(map[string]string)
	lock   sync.Mutex
	index  = 4

	emptyjs   = []byte("{}")
	tabBs     = []byte("\t")
	modelFile = "col.model"
	keys      = []string{}
)

func init() {
	flag.StringVar(&files, "files", "", "file to load")
}

func main() {
	flag.Parse()
	var wg sync.WaitGroup
	for _, f := range strings.Split(files, " ") {
		wg.Add(1)
		go func() {
			process(f)
			wg.Done()
		}()
	}
	wg.Wait()

	saveModel()
	sql := getSql()
	fmt.Println(sql)
}

func process(f string) {
	r, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		js := bytes.Split(sc.Bytes(), tabBs)[index]
		if bytes.Equal(js, emptyjs) {
			continue
		}
		it := make(map[string]interface{})
		err := json.Unmarshal(js, &it)
		if err != nil {
			log.Print(err.Error())
		} else {
			setTag(it)
		}
	}
}

func setTag(it map[string]interface{}) {
	for k, v := range it {
		if _, ok := tagMap[k]; !ok {
			lock.Lock()
			str := "Int32"
			switch v.(type) {
			case string:
				str = "String"
			}
			tagMap[k] = str
			lock.Unlock()
		}
	}
}

func saveModel() {
	for k, _ := range tagMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	f, err := os.OpenFile(modelFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}
	bs, _ := json.Marshal(tagMap)
	f.Write(bs)
	f.Close()
}

func getSql() string {
	sqlTemplate := `CREATE TABLE trend_event
(
    user_id UInt32, 
    timestamp UInt64, 
    event_id UInt32, 
    event_name String, 
    
%s
    event_date Date
)  engine = MergeTree(event_date, (user_id, timestamp, event_date), 8192);
`
	bs := bytes.NewBuffer([]byte{})
	for _, k := range keys {
		v := tagMap[k]
		bs.WriteString("    ")
		bs.WriteString("event_tag_" + k)
		bs.WriteString(" ")
		bs.WriteString(v)
		bs.WriteString(",\n")
	}
	return fmt.Sprintf(sqlTemplate, bs.String())
}

func String2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
