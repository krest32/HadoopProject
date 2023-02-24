show databases;
create database demo;

-- 创建Hbase与Hive关联表
CREATE TABLE hive_hbase_emp_table
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");

DROP TABLE IF EXISTS emp;
CREATE TABLE emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    row format delimited fields terminated by '\t';

show tables;

-- 加载数据到 临时表
load data inpath '/origin_data/demo/emp.txt' into table emp;

select *
from emp;

--  向 Hbase 中插入数据
insert into table hive_hbase_emp_table
select *
from emp;


CREATE EXTERNAL TABLE relevance_hbase_emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    STORED BY
        'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");

-- 可以直接操作 Hive 分析 Hbase 当中的数据
select deptno, avg(sal) monery
from relevance_hbase_emp
group by deptno;