DROP TABLE IF EXISTS dim_sku_full;
CREATE EXTERNAL TABLE dim_sku_full ( `id` STRING COMMENT 'sku_id', `price` DECIMAL(16, 2) COMMENT '商品价格', `sku_name` STRING COMMENT '商品名称', `sku_desc` STRING COMMENT '商品描述', `weight` DECIMAL(16, 2) COMMENT '重量', `is_sale` BOOLEAN COMMENT '是否在售', `spu_id` STRING COMMENT 'spu编号', `spu_name` STRING COMMENT 'spu名称', `category3_id` STRING COMMENT '三级分类id', `category3_name` STRING COMMENT '三级分类名称', `category2_id` STRING COMMENT '二级分类id', `category2_name` STRING COMMENT '二级分类名称', `category1_id` STRING COMMENT '一级分类id', `category1_name` STRING COMMENT '一级分类名称', `tm_id` STRING COMMENT '品牌id', `tm_name` STRING COMMENT '品牌名称', `sku_attr_values` ARRAY < STRUCT < attr_id :STRING, value_id :STRING, attr_name :STRING, value_name:STRING >  > COMMENT '平台属性', `sku_sale_attr_values` ARRAY < STRUCT < sale_attr_id :STRING, sale_attr_value_id :STRING, sale_attr_name :STRING, sale_attr_value_name:STRING >  > COMMENT '销售属性', `create_time` STRING COMMENT '创建时间' ) COMMENT '商品维度表' PARTITIONED BY (`dt` STRING)
-- 列式存储： 提升后期查找性能 STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_sku_full/'
-- 快速的压缩格式：提升后期查找性能 TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载 2020-06-14
WITH sku AS
(
	SELECT  id
	       ,price
	       ,sku_name
	       ,sku_desc
	       ,weight
	       ,is_sale
	       ,spu_id
	       ,category3_id
	       ,tm_id
	       ,create_time
	FROM ods_sku_info_full
	WHERE dt = '2020-06-14'
), spu AS
(
	SELECT  id
	       ,spu_name
	FROM ods_spu_info_full
	WHERE dt = '2020-06-14'
), c3 AS
(
	SELECT  id
	       ,name
	       ,category2_id
	FROM ods_base_category3_full
	WHERE dt = '2020-06-14'
), c2 AS
(
	SELECT  id
	       ,name
	       ,category1_id
	FROM ods_base_category2_full
	WHERE dt = '2020-06-14'
), c1 AS
(
	SELECT  id
	       ,name
	FROM ods_base_category1_full
	WHERE dt = '2020-06-14'
), tm AS
(
	SELECT  id
	       ,tm_name
	FROM ods_base_trademark_full
	WHERE dt = '2020-06-14'
), attr AS
(
	SELECT  sku_id
	       ,collect_set( named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
	FROM ods_sku_attr_value_full
	WHERE dt = '2020-06-14'
	GROUP BY  sku_id
), sale_attr AS
(
	SELECT  sku_id
	       ,collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
	FROM ods_sku_sale_attr_value_full
	WHERE dt = '2020-06-14'
	GROUP BY  sku_id
)
INSERT OVERWRITE TABLE dim_sku_full partition(dt = '2020-06-14')
SELECT  sku.id
       ,sku.price
       ,sku.sku_name
       ,sku.sku_desc
       ,sku.weight
       ,sku.is_sale
       ,sku.spu_id
       ,spu.spu_name
       ,sku.category3_id
       ,c3.name
       ,c3.category2_id
       ,c2.name
       ,c2.category1_id
       ,c1.name
       ,sku.tm_id
       ,tm.tm_name
       ,attr.attrs
       ,sale_attr.sale_attrs
       ,sku.create_time
FROM sku
LEFT JOIN spu
ON sku.spu_id = spu.id
LEFT JOIN c3
ON sku.category3_id = c3.id
LEFT JOIN c2
ON c3.category2_id = c2.id
LEFT JOIN c1
ON c2.category1_id = c1.id
LEFT JOIN tm
ON sku.tm_id = tm.id
LEFT JOIN attr
ON sku.id = attr.sku_id
LEFT JOIN sale_attr
ON sku.id = sale_attr.sku_id;

DROP TABLE IF EXISTS dim_coupon_full;
CREATE EXTERNAL TABLE dim_coupon_full ( `id` STRING COMMENT '购物券编号', `coupon_name` STRING COMMENT '购物券名称', `coupon_type_code` STRING COMMENT '购物券类型编码', `coupon_type_name` STRING COMMENT '购物券类型名称', `condition_amount` DECIMAL(16, 2) COMMENT '满额数', `condition_num` BIGINT COMMENT '满件数', `activity_id` STRING COMMENT '活动编号', `benefit_amount` DECIMAL(16, 2) COMMENT '减金额', `benefit_discount` DECIMAL(16, 2) COMMENT '折扣', `benefit_rule` STRING COMMENT '优惠规则:满元*减*元，满*件打*折', `create_time` STRING COMMENT '创建时间', `range_type_code` STRING COMMENT '优惠范围类型编码', `range_type_name` STRING COMMENT '优惠范围类型名称', `limit_num` BIGINT COMMENT '最多领取次数', `taken_count` BIGINT COMMENT '已领取次数', `start_time` STRING COMMENT '可以领取的开始日期', `end_time` STRING COMMENT '可以领取的结束日期', `operate_time` STRING COMMENT '修改时间', `expire_time` STRING COMMENT '过期时间' ) COMMENT '优惠券维度表' PARTITIONED BY (`dt` STRING) STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_coupon_full/' TBLPROPERTIES ('orc.compress' = 'snappy');
INSERT OVERWRITE TABLE dim_coupon_full partition (dt = '2020-06-14')
SELECT  id
       ,coupon_name
       ,coupon_type
       ,coupon_dic.dic_name
       ,condition_amount
       ,condition_num
       ,activity_id
       ,benefit_amount
       ,benefit_discount
       ,case coupon_type WHEN '3201' THEN concat('满',condition_amount,'元减',benefit_amount,'元') WHEN '3202' THEN concat('满',condition_num,'件打',10 * (1 - benefit_discount),'折') WHEN '3203' THEN concat('减',benefit_amount,'元') end benefit_rule
       ,create_time
       ,range_type
       ,range_dic.dic_name
       ,limit_num
       ,taken_count
       ,start_time
       ,end_time
       ,operate_time
       ,expire_time
FROM
(
	SELECT  id
	       ,coupon_name
	       ,coupon_type
	       ,condition_amount
	       ,condition_num
	       ,activity_id
	       ,benefit_amount
	       ,benefit_discount
	       ,create_time
	       ,range_type
	       ,limit_num
	       ,taken_count
	       ,start_time
	       ,end_time
	       ,operate_time
	       ,expire_time
	FROM ods_coupon_info_full
	WHERE dt = '2020-06-14'
) ci
LEFT JOIN
(
	SELECT  dic_code
	       ,dic_name
	FROM ods_base_dic_full
	WHERE dt = '2020-06-14'
	AND parent_code = '32'
) coupon_dic
ON ci.coupon_type = coupon_dic.dic_code
LEFT JOIN
(
	SELECT  dic_code
	       ,dic_name
	FROM ods_base_dic_full
	WHERE dt = '2020-06-14'
	AND parent_code = '33'
) range_dic
ON ci.range_type = range_dic.dic_code;

DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full ( `activity_rule_id` STRING COMMENT '活动规则ID', `activity_id` STRING COMMENT '活动ID', `activity_name` STRING COMMENT '活动名称', `activity_type_code` STRING COMMENT '活动类型编码', `activity_type_name` STRING COMMENT '活动类型名称', `activity_desc` STRING COMMENT '活动描述', `start_time` STRING COMMENT '开始时间', `end_time` STRING COMMENT '结束时间', `create_time` STRING COMMENT '创建时间', `condition_amount` DECIMAL(16, 2) COMMENT '满减金额', `condition_num` BIGINT COMMENT '满减件数', `benefit_amount` DECIMAL(16, 2) COMMENT '优惠金额', `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣', `benefit_rule` STRING COMMENT '优惠规则', `benefit_level` STRING COMMENT '优惠级别' ) COMMENT '活动信息表' PARTITIONED BY (`dt` STRING) STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_activity_full/' TBLPROPERTIES ('orc.compress' = 'snappy');
INSERT OVERWRITE TABLE dim_activity_full partition (dt = '2020-06-14')
SELECT  rule.id
       ,info.id
       ,activity_name
       ,rule.activity_type
       ,dic.dic_name
       ,activity_desc
       ,start_time
       ,end_time
       ,create_time
       ,condition_amount
       ,condition_num
       ,benefit_amount
       ,benefit_discount
       ,case rule.activity_type WHEN '3101' THEN concat('满',condition_amount,'元减',benefit_amount,'元') WHEN '3102' THEN concat('满',condition_num,'件打',10 * (1 - benefit_discount),'折') WHEN '3103' THEN concat('打',10 * (1 - benefit_discount),'折') end benefit_rule
       ,benefit_level
FROM
(
	SELECT  id
	       ,activity_id
	       ,activity_type
	       ,condition_amount
	       ,condition_num
	       ,benefit_amount
	       ,benefit_discount
	       ,benefit_level
	FROM ods_activity_rule_full
	WHERE dt = '2020-06-14'
) rule
LEFT JOIN
(
	SELECT  id
	       ,activity_name
	       ,activity_type
	       ,activity_desc
	       ,start_time
	       ,end_time
	       ,create_time
	FROM ods_activity_info_full
	WHERE dt = '2020-06-14'
) info
ON rule.activity_id = info.id
LEFT JOIN
(
	SELECT  dic_code
	       ,dic_name
	FROM ods_base_dic_full
	WHERE dt = '2020-06-14'
	AND parent_code = '31'
) dic
ON rule.activity_type = dic.dic_code;

DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full ( `id` STRING COMMENT 'id', `province_name` STRING COMMENT '省市名称', `area_code` STRING COMMENT '地区编码', `iso_code` STRING COMMENT '旧版ISO-3166-2编码，供可视化使用', `iso_3166_2` STRING COMMENT '新版IOS-3166-2编码，供可视化使用', `region_id` STRING COMMENT '地区id', `region_name` STRING COMMENT '地区名称' ) COMMENT '地区维度表' PARTITIONED BY (`dt` STRING) STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_province_full/' TBLPROPERTIES ('orc.compress' = 'snappy');
INSERT OVERWRITE TABLE dim_province_full partition (dt = '2020-06-14')
SELECT  province.id
       ,province.name
       ,province.area_code
       ,province.iso_code
       ,province.iso_3166_2
       ,region_id
       ,region_name
FROM
(
	SELECT  id
	       ,name
	       ,region_id
	       ,area_code
	       ,iso_code
	       ,iso_3166_2
	FROM ods_base_province_full
	WHERE dt = '2020-06-14'
) province
LEFT JOIN
(
	SELECT  id
	       ,region_name
	FROM ods_base_region_full
	WHERE dt = '2020-06-14'
) region
ON province.region_id = region.id;

DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date ( `date_id` STRING COMMENT '日期ID', `week_id` STRING COMMENT '周ID,一年中的第几周', `week_day` STRING COMMENT '周几', `day` STRING COMMENT '每月的第几天', `month` STRING COMMENT '一年中的第几月', `quarter` STRING COMMENT '一年中的第几季度', `year` STRING COMMENT '年份', `is_workday` STRING COMMENT '是否是工作日', `holiday_id` STRING COMMENT '节假日' ) COMMENT '时间维度表' STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_date/' TBLPROPERTIES ('orc.compress' = 'snappy');

DROP TABLE IF EXISTS tmp_dim_date_info;
CREATE EXTERNAL TABLE tmp_dim_date_info ( `date_id` STRING COMMENT '日', `week_id` STRING COMMENT '周ID', `week_day` STRING COMMENT '周几', `day` STRING COMMENT '每月的第几天', `month` STRING COMMENT '第几月', `quarter` STRING COMMENT '第几季度', `year` STRING COMMENT '年', `is_workday` STRING COMMENT '是否是工作日', `holiday_id` STRING COMMENT '节假日' ) COMMENT '时间维度表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info/';
INSERT OVERWRITE TABLE dim_date
SELECT  *
FROM tmp_dim_date_info;

DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip ( `id` STRING COMMENT '用户id', `login_name` STRING COMMENT '用户名称', `nick_name` STRING COMMENT '用户昵称', `name` STRING COMMENT '用户姓名', `phone_num` STRING COMMENT '手机号码', `email` STRING COMMENT '邮箱', `user_level` STRING COMMENT '用户等级', `birthday` STRING COMMENT '生日', `gender` STRING COMMENT '性别', `create_time` STRING COMMENT '创建时间', `operate_time` STRING COMMENT '操作时间', `start_date` STRING COMMENT '开始日期', `end_date` STRING COMMENT '结束日期' ) COMMENT '用户表' PARTITIONED BY (`dt` STRING) STORED AS ORC LOCATION '/warehouse/gmall/dim/dim_user_zip/' TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日装载
INSERT OVERWRITE TABLE dim_user_zip partition (dt = '9999-12-31')
SELECT  data.id
       ,data.login_name
       ,data.nick_name
       ,md5(data.name)
       ,md5(data.phone_num)
       ,md5(data.email)
       ,data.user_level
       ,data.birthday
       ,data.gender
       ,data.create_time
       ,data.operate_time
       ,'2020-06-14' start_date
       ,'9999-12-31' end_date
FROM ods_user_info_inc
WHERE dt = '2020-06-14'
AND type = 'bootstrap-insert';

SET hive.exec.dynamic.partition.mode = nonstrict;
WITH tmp AS
(
	SELECT  old.id old_id
	       ,old.login_name old_login_name
	       ,old.nick_name old_nick_name
	       ,old.name old_name
	       ,old.phone_num old_phone_num
	       ,old.email old_email
	       ,old.user_level old_user_level
	       ,old.birthday old_birthday
	       ,old.gender old_gender
	       ,old.create_time old_create_time
	       ,old.operate_time old_operate_time
	       ,old.start_date old_start_date
	       ,old.end_date old_end_date
	       ,new.id new_id
	       ,new.login_name new_login_name
	       ,new.nick_name new_nick_name
	       ,new.name new_name
	       ,new.phone_num new_phone_num
	       ,new.email new_email
	       ,new.user_level new_user_level
	       ,new.birthday new_birthday
	       ,new.gender new_gender
	       ,new.create_time new_create_time
	       ,new.operate_time new_operate_time
	       ,new.start_date new_start_date
	       ,new.end_date new_end_date
	FROM
	(
		SELECT  id
		       ,login_name
		       ,nick_name
		       ,name
		       ,phone_num
		       ,email
		       ,user_level
		       ,birthday
		       ,gender
		       ,create_time
		       ,operate_time
		       ,start_date
		       ,end_date
		FROM dim_user_zip
		WHERE dt = '9999-12-31'
	) old
	FULL OUTER JOIN
	(
		SELECT  id
		       ,login_name
		       ,nick_name
		       ,md5(name) name
		       ,md5(phone_num) phone_num
		       ,md5(email) email
		       ,user_level
		       ,birthday
		       ,gender
		       ,create_time
		       ,operate_time
		       ,'2020-06-15' start_date
		       ,'9999-12-31' end_date
		FROM
		(
			SELECT  data.id
			       ,data.login_name
			       ,data.nick_name
			       ,data.name
			       ,data.phone_num
			       ,data.email
			       ,data.user_level
			       ,data.birthday
			       ,data.gender
			       ,data.create_time
			       ,data.operate_time
			       ,row_number() over (partition by data.id ORDER BY ts desc) rn
			FROM ods_user_info_inc
			WHERE dt = '2020-06-15'
		) t1
		WHERE rn = 1
	) new
	ON old.id = new.id
)
INSERT OVERWRITE TABLE dim_user_zip partition ( dt )
SELECT  if(new_id is not null,new_id,old_id)
       ,if(new_id is not null,new_login_name,old_login_name)
       ,if(new_id is not null,new_nick_name,old_nick_name)
       ,if(new_id is not null,new_name,old_name)
       ,if(new_id is not null,new_phone_num,old_phone_num)
       ,if(new_id is not null,new_email,old_email)
       ,if(new_id is not null,new_user_level,old_user_level)
       ,if(new_id is not null,new_birthday,old_birthday)
       ,if(new_id is not null,new_gender,old_gender)
       ,if(new_id is not null,new_create_time,old_create_time)
       ,if(new_id is not null,new_operate_time,old_operate_time)
       ,if(new_id is not null,new_start_date,old_start_date)
       ,if(new_id is not null,new_end_date,old_end_date)
       ,if(new_id is not null,new_end_date,old_end_date) dt
FROM tmp
UNION ALL
SELECT  old_id
       ,old_login_name
       ,old_nick_name
       ,old_name
       ,old_phone_num
       ,old_email
       ,old_user_level
       ,old_birthday
       ,old_gender
       ,old_create_time
       ,old_operate_time
       ,old_start_date
       ,cast(date_add('2020-06-15',-1) AS string) old_end_date
       ,cast(date_add('2020-06-15',-1) AS string) dt
FROM tmp
WHERE old_id is not null
AND new_id is not null;