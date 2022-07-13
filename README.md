# ssb-spark

## 依赖
- juicefs-1.0-beata2
- spark-3.2.1-bin-hadoop2.7
- mysql-5.7
- MacOS

## juicefs环境搭建
这里使用mysql做juicefs的元数据，而且也用做juicefs的存储，方便测试。

1. mysql中创建数据库

![](http://image-picgo.test.upcdn.net/img/20220713144305.png)


2. 解压juicefs-1.0.0-rc2-darwin-amd64.tar.gz

3. 初始化juicefs
```
./juicefs format  --storage mysql \
    --bucket (localhost:3306)/juicefs2_bucket \
    --access-key root \
    --secret-key eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT \
    "mysql://root:eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT@(localhost:3306)/juicefs2_meta" \
    myjfs
```
成功后如图：

![](http://image-picgo.test.upcdn.net/img/20220713144611.png)

## 生成数据
挂载ssb-benchmark目录到docker内

```
cd ssb-benchmark
docker run -v $PWD:/opt/ssb -i -t  centos:7.9.2009 /bin/bash 
```
进入docker内后，下载依赖
```
yum -y install gcc automake autoconf libtool make
```


在容器内执行数据集生成。
```
cd /opt/ssb/ 
make clean 
make
./dbgen -vfF -s 0.01 -T s
./dbgen -vfF -s 0.01 -T d
./dbgen -vfF -s 0.01 -T p
./dbgen -vfF -s 0.01 -T c
./dbgen -vfF -s 0.01 -T l
chmod +r lineorder.tbl 
```

## 上传ssb数据集到juicefs

1. 将juicefs挂载到mac的本地目录

```
./juicefs mount "mysql://root:eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT@(localhost:3306)/juicefs2_meta" mnt
```
![](http://image-picgo.test.upcdn.net/img/20220713144808.png)

2. 先在juicefs文件系统里创建ssb目录

```
~/Downloads/juicefs/mnt » mkdir ssb
```

3. 拷贝ssb生成的文件到juicefs文件系统里
```
cd ssb-benchmark
cp *.tbl ~/Downloads/juicefs/mnt/ssb
```

## 用juicefs Gateway来查看juicefs文件系统数据
启动juicefs Gateway。
```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123456
./juicefs gateway "mysql://root:eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT@(localhost:3306)/juicefs2_meta" localhost:9001

```
![](http://image-picgo.test.upcdn.net/img/20220713145716.png)

## 用spark将ssb数据集导入成Iceberg表
1. 启动spark-shell

```
  bin/spark-shell --jars iceberg-spark-runtime-3.2_2.12-0.13.2.jar,juicefs-hadoop-1.0.0-rc2.jar \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/iceberg_warehouse \
    --conf spark.hadoop.fs.jfs.impl=io.juicefs.JuiceFileSystem  \
    --conf spark.hadoop.fs.defaultFS=jfs://myjfs/ \
    --conf spark.hadoop.fs.AbstractFileSystem.jfs.impl=io.juicefs.JuiceFS  \
    --conf spark.hadoop.juicefs.meta=mysql://root:eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT@(localhost:3306)/juicefs2_meta

```

2. 在spark-shell终端中输入`:paste`
将load.scala里的代码贴进去终端就可以将ssb数据集导入Iceberg表里。
用spark-sql进行Iceberg表分析。

```
  bin/spark-sql --jars iceberg-spark-runtime-3.2_2.12-0.13.2.jar,juicefs-hadoop-1.0.0-rc2.jar \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/iceberg_warehouse \
    --conf spark.hadoop.fs.jfs.impl=io.juicefs.JuiceFileSystem  \
    --conf spark.hadoop.fs.defaultFS=jfs://myjfs/ \
    --conf spark.hadoop.fs.AbstractFileSystem.jfs.impl=io.juicefs.JuiceFS  \
    --conf spark.hadoop.juicefs.meta=mysql://root:eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT@(localhost:3306)/juicefs2_meta

```
进入sql终端进行数据分析。
```
spark-sql> use local.default;
Time taken: 0.094 seconds

spark-sql> show tables;
lineorder
customer
date
part
supplier


```
然后可以使用sql目录的语句进行分析。

![](http://image-picgo.test.upcdn.net/img/20220713202236.png)
