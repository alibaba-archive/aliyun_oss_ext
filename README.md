# Greenplum parallel import and export OSS data plugin oss_ext

Greenplum supports parallel import from OSS or export to OSS through external tables (which is called the gpossext function). It can also compress OSS external table files in gzip format to reduce the storage space and the costs.

The gpossext function can read or write text/csv files or text/csv files in gzip format.

## 1\.user documentation

[oss_ext english documentation][2]

## 2\.what is oss

Alibaba Cloud Object Storage Service ([OSS][6]) is a storage service that enables you to store, back up, and archive any amount of data in the cloud. OSS is a cost-effective, highly secure, and highly reliable cloud storage solution. It uses RESTful APIs and is designed for 99.999999999% (11 nines) durability and 99.99% availability. Using OSS, you can store and retrieve any type of data at any time, from anywhere on the web.

You can use API and SDK interfaces provided by Alibaba Cloud or OSS migration tools to transfer massive amounts of data into or out of Alibaba Cloud OSS. You can use the Standard storage class of OSS to store image, audio, and video files for apps and large websites. You can use the Infrequent Access (IA) or Archive storage class as a low-cost solution for backup and archiving of infrequently accessed data.


## 3\.dependency

### build dependency

1\. [oss c sdk][4] 

the stable version of osslib already included in the code

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

Pigz is installed into the Greenplum bin directory to compress and write data from Greenplum to oss

4\. testcase dependency

aliyun [osscmd][3]

osscmd [object commands][1]

### 4\.performance

The performance of oss_ext read and write oss increases with the increase of Greenplum compute nodes. It supports asynchronous reading of data in oss and parallel compression of data to write oss.

The oss has a traffic limit of about 5Gbyte/s. If there is a demand, you can request bandwidth from the oss product.



[1]:https://www.alibabacloud.com/help/doc-detail/32187.htm?spm=a2c63.p38356.a1.4.187d208dkoG7kz
[2]:https://www.alibabacloud.com/help/doc-detail/35457.htm?spm=a2c63.l28256.b99.18.601645b6EdxaRK
[3]:https://www.alibabacloud.com/help/doc-detail/32184.htm?spm=a2c63.p38356.a3.5.15e073cbYxQWZO#concept-jv4-ssb-wdb
[4]:https://github.com/aliyun/aliyun-oss-c-sdk
[5]:http://zlib.net/pigz/
[6]:https://www.alibabacloud.com/help/doc-detail/31817.htm?spm=a2c63.l28256.a3.2.452f5139OuU3vS

