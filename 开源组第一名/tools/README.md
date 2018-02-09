数据导入、测试文档
---

#### 环境准备
- 参考上级目录README文档编译部署好ClickHouse服务
- 安装好Go环境,方便处理数据

#### 数据处理

- 安装三方json包,节省导入效率
```
## 安装三方json包,节省导入效率 
go get -u github.com/json-iterator/go

```

-  读一遍数据文件识别动态Scheme生成sql和模型文件
```
go run createtable.go  -files=`ls 2017*` | tee  create_table.sql`
```

- 通过模型文件,处理数据, 将数据存入 output目录
```
## 数据如果单节点硬盘存不下,可以将数据按月份分散到多台机器,修改 `ls 2017XXX`来处理数据
output="`pwd`/output"
mkdir -p $output
for f in `ls 2017*`;do
	go run index.go -file="$f" -out="`pwd`/output"   
done
```

- 生成csv文件
```
	## 正式数据,user_id内容需要去掉前缀id,提高group效率
	## prefix参数,2017表示要处理数据文件名的前缀,如果是多台机器分散处理,合理修改此参数 
	sh to_csv.sh 2017
```


- 导入数据
```
 ## prefix参数,2017表示要处理数据文件名的前缀,如果是多台机器分散处理,合理修改此参数 
 sh import.sh 2017
```

- 按月合并数据块,提高查询效率
```
	# https://clickhouse.yandex/docs/en/query_language/queries.html#optimize
	# 依次在各个节点sql交互环境下执行以下命令,按月合并数据块
	OPTIMIZE TABLE event PARTITION 201706 FINAL;
	OPTIMIZE TABLE event PARTITION 201707 FINAL;
	OPTIMIZE TABLE event PARTITION 201708 FINAL;
```

- 查询
	- 测试sql:7,8月份, 转化路径 10004,10008, 10009,10010, 且10004事件的标签品牌是 Apple或者LianX,  时间窗口为30天的漏斗情况
	- 修改q.sh中的clients变量值, 设置为各个节点的hosts
	- 参考a.sql文件示例,执行:`sh query_sql.sh a.sql`
	- 结果 `4000000 4000000 3999994 3999936`
	



