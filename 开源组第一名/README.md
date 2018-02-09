## 使用方法：

去github拉取ClickHouse源码，然后添加修改

```
git clone git@github.com:yandex/ClickHouse.git
cd ClickHouse
git checkout ab7672f329f7736756542268178e6f9f7e32325a
git checkout -b path
git apply 0001-Add-AggregateFunctionPath.patch
```


## 编译方法：
按照文档，安装所有依赖，https://clickhouse.yandex/docs/en/development/build.html

然后使用以下命令编译出 clickhouse 文件

```
mkdir build
cd build
cmake ..
make -j 8 clickhouse
ls dbms/src/Server/clickhouse
```

可执行文件：
dbms/src/Server/clickhouse 





## 部署方法：

部署方法：

每一个目标节点安装依赖：

```
sudo yum -y install rpm-build redhat-rpm-config gcc-c++ readline-devel\
  unixODBC-devel subversion python-devel git wget openssl-devel m4 createrepo\
  libicu-devel zlib-devel libtool-ltdl-devel
```

然后把 clickhouse 文件放到 /data/ccc/ 目录，命名为 ccc

```
mkdir -p /data/ccc/
cp clickhouse /data/ccc/ccc
```

把源码的 dbms/src/Server/config.xml 放到 /data/ccc/ 目录，注意修改里面的相关配置：

```
<path>/var/lib/clickhouse/</path> 指定存储目录
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path> 指定临时目录
以下指定监听端口和地址
    <tcp_port>9000</tcp_port>

    <!-- <listen_host>::</listen_host> -->
    <listen_host>::1</listen_host>
    <listen_host>127.0.0.1</listen_host>
```

启动Server：
`./ccc --server --config-file=/data/ccc/config.xml`

启动Client：
`./ccc --client --host 127.0.0.1 --port 9000`



## 数据导入：

在每一个clickhouse节点建本地表：

```
CREATE TABLE event (
user_id UInt32,
timestamp_nc UInt64,
event_id_nc UInt32,
event_name String,
event_tag_brand String,
event_tag_content String,
event_tag_how Int32,
event_tag_page_num Int32,
event_tag_price Int32,
event_tag_price_all Int32,
event_date_nc Date) 
ENGINE = MergeTree(event_date_nc, (user_id, timestamp_nc, event_date_nc), 8192);
```

然后在每一个clickhouse节点建 分布式表：

```
CREATE TABLE dist_event (
user_id UInt32,
timestamp_nc UInt64,
event_id_nc UInt32,
event_name String,
event_tag_brand String,
event_tag_content String,
event_tag_how Int32,
event_tag_page_num Int32,
event_tag_price Int32,
event_tag_price_all Int32,
event_date_nc Date) 
ENGINE = Distributed(default, default, event, user_id);
```

分布式表需要对配置文件添加的 <remote_servers> 配置，参考 https://clickhouse.yandex/docs/en/table_engines/distributed.html

然后使用 tools 文件夹里面的工具，把数据文件处理之后，导入 dist_event 表。参考 `tools/README.md`


