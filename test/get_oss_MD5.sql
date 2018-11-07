create extension if not exists oss_ext;
drop external table if exists get_oss_MD5 ;
create READABLE external table get_oss_MD5 (a int) location('oss://oss-cn-hangzhou.aliyuncs.com filepath=oss_reg_test/example.csv.1 id=xxx key=xxx bucket=osshuadong1') FORMAT 'csv';
\d+ get_oss_MD5
drop external table if exists get_oss_MD5 ;
drop extension oss_ext;
