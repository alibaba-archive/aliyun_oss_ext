# Greenplum 读写阿里云 OSS 外部表插件 oss_ext

oss_ext 支持将数据并行从 oss 导入或导出到 Greenplum，大量节省本地存储空间及成本。

目前的 oss_ext 支持读写text/csv 格式的文件或者 gzip 压缩格式的 text/csv 文件。

## 用户文档

[oss_ext 完整的阿里云中文官方][1]

[oss_ext english doc][2]

## 依赖

### 源码依赖

1\. [oss c sdk][4]

2\. libs

```
apr
apr-devel
apr-util
apr-util-devel
mxml
mxml-devel
```

3\. [pigz][5]

该模块安装到 Greenplum bin 目录，用于把 Greenplum 读的数据并行压缩并写到 oss。

### 回归测试依赖

aliyun [osscmd][3]

### 性能

oss_ext 读写随 Greenplum 计算节点增加而增加，支持异步读 oss 中的数据和并行压缩数据后写 oss。

oss 有一个流量限制，约 5Gbyte/s ，如果有特殊需求可以向 oss 产品请求增加带宽。



[1]:https://help.aliyun.com/document_detail/35457.html?spm=5176.11065259.1996646101.searchclickresult.59eb771fs1fGIl
[2]:https://www.alibabacloud.com/help/doc-detail/35457.htm?spm=a2c63.l28256.b99.18.601645b6EdxaRK
[3]:https://help.aliyun.com/document_detail/32184.html?spm=a2c4g.11186623.6.1250.28991db0BEHe25
[4]:https://github.com/aliyun/aliyun-oss-c-sdk
[5]:http://zlib.net/pigz/
