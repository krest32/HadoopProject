show databases;

use default;

show tables ;


select * from default.student2 where id >0;


drop table t_archer;
--ddl create table
create table t_archer(
                         id int comment "ID",
                         name string comment "英雄名称",
                         hp_max int comment "最大生命",
                         mp_max int comment "最大法力",
                         attack_max int comment "最高物攻",
                         defense_max int comment "最大物防",
                         attack_range string comment "攻击范围",
                         role_main string comment "主要定位",
                         role_assist string comment "次要定位"
) comment "王者荣耀射手信息"
    row format delimited fields terminated by "\t";

select * from default.t_archer where hp_max>6000;


create table t_hot_hero_skin_price(
                                      id int,
                                      name string,
                                      win_rate int,
                                      skin_price map<string,int>
)
    row format
        delimited fields terminated by ',' --字段之间分隔符
        collection items terminated by '-'  --集合元素之间分隔符
        map keys terminated by ':'; --集合元素kv之间分隔符;


create table t_team_ace_player(
                                  id int,
                                  team_name string,
                                  ace_player_name string
); --没有指定row format语句 此时采用的是默认的\001作为字段的分隔符

select * from t_team_ace_player;



select * from default.t_hot_hero_skin_price

create table t_all_hero(
   id int,
   name string,
   hp_max int,
   mp_max int,
   attack_max int,
   defense_max int,
   attack_range string,
   role_main string,
   role_assist string
) row format delimited fields terminated by "\t";


select * from default.t_all_hero;

drop table t_all_hero;

--注意分区表创建语法规则
--分区表建表
create table t_all_hero_part(
    id int,
    name string,
    hp_max int,
    mp_max int,
    attack_max int,
    defense_max int,
    attack_range string,
    role_main string,
    role_assist string
) partitioned by (role string)--注意哦 这里是分区字段
    row format delimited
        fields terminated by "\t";


drop table t_all_hero_part;

select * from t_all_hero_part;


load data inpath '/user/hive/warehouse/t_all_hero_part/archer.txt' into table t_all_hero_part partition(role='sheshou');
load data inpath '/user/hive/warehouse/t_all_hero_part/assassin.txt' into table t_all_hero_part partition(role='cike');
load data inpath '/user/hive/warehouse/t_all_hero_part/mage.txt' into table t_all_hero_part partition(role='fashi');
load data inpath '/user/hive/warehouse/t_all_hero_part/support.txt' into table t_all_hero_part partition(role='fuzhu');
load data inpath '/user/hive/warehouse/t_all_hero_part/tank.txt' into table t_all_hero_part partition(role='tanke');
load data inpath '/user/hive/warehouse/t_all_hero_part/warrior.txt' into table t_all_hero_part partition(role='zhanshi');


select count(*) from t_all_hero_part where role="sheshou" and hp_max >6000;


create table t_user_province_city (id int, name string,age int) partitioned by (province string, city string);

load data local inpath '/user/hive/warehouse/t_user_province_city/user..txt' into table t_user_province_city
    partition(province='zhejiang',city='hangzhou');
load data local inpath '/usr/local/krest/hivedata/user..txt' into table t_user_province_city
    partition(province='zhejiang',city='ningbo');
load data local inpath '/usr/local/krest/hivedata/user..txt' into table t_user_province_city
    partition(province='shanghai',city='pudong');

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 设置允许动态分区最大分区数量
Set hive.exec.max.dynamic.partitions=1000;


create table t_all_hero_part_dynamic(
    id int,
    name string,
    hp_max int,
    mp_max int,
    attack_max int,
    defense_max int,
    attack_range string,
    role_main string,
    role_assist string
) partitioned by (role string)
    row format delimited fields terminated by "\t";

select * from t_all_hero;

insert into table t_all_hero_part_dynamic partition(role) --注意这里 分区值并没有手动写死指定
select tmp.*,tmp.role_main from t_all_hero tmp;


select * from t_all_hero_part_dynamic;


CREATE TABLE t_usa_covid19_bucket(
     count_date string,
     county string,
     state string,
     fips int,
     cases int,
     deaths int)
    CLUSTERED BY(state) INTO 5 BUCKETS; --分桶的字段一定要是表中已经存在的字段

--根据state州分为5桶 每个桶内根据cases确诊病例数倒序排序
CREATE TABLE t_usa_covid19_bucket_sort(
                                                  count_date string,
                                                  county string,
                                                  state string,
                                                  fips int,
                                                  cases int,
                                                  deaths int)
    CLUSTERED BY(state)
        sorted by (cases desc) INTO 5 BUCKETS;--指定每个分桶内部根据 cases倒序排序


--step1:开启分桶的功能 从Hive2.0开始不再需要设置
set hive.enforce.bucketing=true;



drop table if exists t_usa_covid19;
CREATE TABLE t_usa_covid19(
      count_date string,
      county string,
      state string,
      fips int,
      cases int,
      deaths int)
    row format delimited fields terminated by ",";


insert into t_usa_covid19_bucket select * from t_usa_covid19;

set hive.exec.mode.local.auto=false;


select * from t_usa_covid19_bucket;


select * from t_usa_covid19_bucket where state="New York";



CREATE TABLE hive_hbase_emp_table(
                                     empno int,
                                     ename string,
                                     job string,
                                     mgr int,
                                     hiredate string,
                                     sal double,
                                     comm double,
                                     deptno int
)STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");



CREATE TABLE emp(
                    empno int,
                    ename string,
                    job string,
                    mgr int,
                    hiredate string,
                    sal double,
                    comm double,
                    deptno int
)
    row format delimited fields terminated by '\t';


select * from emp;


insert into table hive_hbase_emp_table select * from emp;


select * from hive_hbase_emp_table;

CREATE EXTERNAL TABLE relevance_hbase_emp(
                                             empno int,
                                             ename string,
                                             job string,
                                             mgr int,
                                             hiredate string,
                                             sal double,comm double,
                                             deptno int
)
    STORED BY
        'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:co
            mm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");


select deptno,avg(sal) monery from relevance_hbase_emp group by deptno ;