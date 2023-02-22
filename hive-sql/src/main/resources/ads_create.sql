DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `channel` STRING COMMENT '渠道', `uv_count` BIGINT COMMENT '访客人数', `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒', `avg_page_count` BIGINT COMMENT '会话平均浏览页面数', `sv_count` BIGINT COMMENT '会话数', `bounce_rate` DECIMAL(16, 2) COMMENT '跳出率' ) COMMENT '各渠道流量统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';
INSERT OVERWRITE TABLE ads_traffic_stats_by_channel
SELECT  *
FROM ads_traffic_stats_by_channel
UNION
SELECT  '2020-06-14' dt
       ,recent_days
       ,channel
       ,cast(COUNT(distinct (mid_id))                  AS bigint) uv_count
       ,cast(AVG(during_time_1d) / 1000                AS bigint) avg_duration_sec
       ,cast(AVG(page_count_1d)                        AS bigint) avg_page_count
       ,cast(COUNT(*)                                  AS bigint) sv_count
       ,cast(SUM(if(page_count_1d = 1,1,0)) / COUNT(*) AS decimal(16,2)) bounce_rate
FROM dws_traffic_session_page_view_1d lateral view explode(array(1, 7, 30)) tmp AS recent_days
WHERE dt >= date_add('2020-06-14', -recent_days + 1)
GROUP BY  recent_days
         ,channel;

DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `source` STRING COMMENT '跳转起始页面ID', `target` STRING COMMENT '跳转终到页面ID', `path_count` BIGINT COMMENT '跳转次数' ) COMMENT '页面浏览路径分析' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_page_path/';
INSERT OVERWRITE TABLE ads_page_path
SELECT  *
FROM ads_page_path
UNION
SELECT  '2020-06-14' dt
       ,recent_days
       ,source
       ,nvl(target,'null')
       ,COUNT(*) path_count
FROM
(
	SELECT  recent_days
	       ,concat('step-',rn,':',page_id) source
	       ,concat('step-',rn + 1,':',next_page_id) target
	FROM
	(
		SELECT  recent_days
		       ,page_id
		       ,lead(page_id,1,null) over (partition by session_id,recent_days) next_page_id
		       ,row_number() over (partition by session_id,recent_days ORDER BY view_time) rn
		FROM dwd_traffic_page_view_inc lateral view explode(array(1, 7, 30)) tmp AS recent_days
		WHERE dt >= date_add('2020-06-14', -recent_days + 1)
	) t1
) t2
GROUP BY  recent_days
         ,source
         ,target;

DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change ( `dt` STRING COMMENT '统计日期', `user_churn_count` BIGINT COMMENT '流失用户数', `user_back_count` BIGINT COMMENT '回流用户数' ) COMMENT '用户变动统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_change/';
INSERT OVERWRITE TABLE ads_user_change
SELECT  *
FROM ads_user_change
UNION
SELECT  churn.dt
       ,user_churn_count
       ,user_back_count
FROM
(
	SELECT  '2020-06-14' dt
	       ,COUNT(*) user_churn_count
	FROM dws_user_user_login_td
	WHERE dt = '2020-06-14'
	AND login_date_last = date_add('2020-06-14', -7)
) churn
JOIN
(
	SELECT  '2020-06-14' dt
	       ,COUNT(*) user_back_count
	FROM
	(
		SELECT  user_id
		       ,login_date_last
		FROM dws_user_user_login_td
		WHERE dt = '2020-06-14'
	) t1
	JOIN
	(
		SELECT  user_id
		       ,login_date_last login_date_previous
		FROM dws_user_user_login_td
		WHERE dt = date_add('2020-06-14', -1)
	) t2
	ON t1.user_id = t2.user_id
	WHERE DATEDIFF(login_date_last, login_date_previous) >= 8
) back
ON churn.dt = back.dt;

DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention ( `dt` STRING COMMENT '统计日期', `create_date` STRING COMMENT '用户新增日期', `retention_day` INT COMMENT '截至当前日期留存天数', `retention_count` BIGINT COMMENT '留存用户数量', `new_user_count` BIGINT COMMENT '新增用户数量', `retention_rate` DECIMAL(16, 2) COMMENT '留存率' ) COMMENT '用户留存率' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_retention/';
INSERT OVERWRITE TABLE ads_user_retention
SELECT  *
FROM ads_user_retention
UNION
SELECT  '2020-06-14' dt
       ,login_date_first create_date
       ,DATEDIFF('2020-06-14',login_date_first) retention_day
       ,SUM(if(login_date_last = '2020-06-14',1,0)) retention_count
       ,COUNT(*) new_user_count
       ,cast(SUM(if(login_date_last = '2020-06-14',1,0)) / COUNT(*) * 100 AS decimal(16,2)) retention_rate
FROM
(
	SELECT  user_id
	       ,date_id login_date_first
	FROM dwd_user_register_inc
	WHERE dt >= date_add('2020-06-14', -7)
	AND dt < '2020-06-14'
) t1
JOIN
(
	SELECT  user_id
	       ,login_date_last
	FROM dws_user_user_login_td
	WHERE dt = '2020-06-14'
) t2
ON t1.user_id = t2.user_id
GROUP BY  login_date_first;

DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日', `new_user_count` BIGINT COMMENT '新增用户数', `active_user_count` BIGINT COMMENT '活跃用户数' ) COMMENT '用户新增活跃统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_stats/';
INSERT OVERWRITE TABLE ads_user_stats
SELECT  *
FROM ads_user_stats
UNION
SELECT  '2020-06-14' dt
       ,t1.recent_days
       ,new_user_count
       ,active_user_count
FROM
(
	SELECT  recent_days
	       ,SUM(if(login_date_last >= date_add('2020-06-14',-recent_days + 1),1,0)) new_user_count
	FROM dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp AS recent_days
	WHERE dt = '2020-06-14'
	GROUP BY  recent_days
) t1
JOIN
(
	SELECT  recent_days
	       ,SUM(if(date_id >= date_add('2020-06-14',-recent_days + 1),1,0)) active_user_count
	FROM dwd_user_register_inc lateral view explode(array(1, 7, 30)) tmp AS recent_days
	GROUP BY  recent_days
) t2
ON t1.recent_days = t2.recent_days;

DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `home_count` BIGINT COMMENT '浏览首页人数', `good_detail_count` BIGINT COMMENT '浏览商品详情页人数', `cart_count` BIGINT COMMENT '加入购物车人数', `order_count` BIGINT COMMENT '下单人数', `payment_count` BIGINT COMMENT '支付人数' ) COMMENT '漏斗分析' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_user_action/';
INSERT OVERWRITE TABLE ads_user_action
SELECT  *
FROM ads_user_action
UNION
SELECT  '2020-06-14' dt
       ,page.recent_days
       ,home_count
       ,good_detail_count
       ,cart_count
       ,order_count
       ,payment_count
FROM
(
	SELECT  1 recent_days
	       ,SUM(if(page_id = 'home',1,0)) home_count
	       ,SUM(if(page_id = 'good_detail',1,0)) good_detail_count
	FROM dws_traffic_page_visitor_page_view_1d
	WHERE dt = '2020-06-14'
	AND page_id IN ('home', 'good_detail')
	UNION ALL
	SELECT  recent_days
	       ,SUM(if(page_id = 'home' AND view_count > 0,1,0))
	       ,SUM(if(page_id = 'good_detail' AND view_count > 0,1,0))
	FROM
	(
		SELECT  recent_days
		       ,page_id
		       ,case recent_days WHEN 7 THEN view_count_7d WHEN 30 THEN view_count_30d end view_count
		FROM dws_traffic_page_visitor_page_view_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
		AND page_id IN ('home', 'good_detail')
	) t1
	GROUP BY  recent_days
) page
JOIN
(
	SELECT  1 recent_days
	       ,COUNT(*) cart_count
	FROM dws_trade_user_cart_add_1d
	WHERE dt = '2020-06-14'
	UNION ALL
	SELECT  recent_days
	       ,SUM(if(cart_count > 0,1,0))
	FROM
	(
		SELECT  recent_days
		       ,case recent_days WHEN 7 THEN cart_add_count_7d WHEN 30 THEN cart_add_count_30d end cart_count
		FROM dws_trade_user_cart_add_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
) cart
ON page.recent_days = cart.recent_days
JOIN
(
	SELECT  1 recent_days
	       ,COUNT(*) order_count
	FROM dws_trade_user_order_1d
	WHERE dt = '2020-06-14'
	UNION ALL
	SELECT  recent_days
	       ,SUM(if(order_count > 0,1,0))
	FROM
	(
		SELECT  recent_days
		       ,case recent_days WHEN 7 THEN order_count_7d WHEN 30 THEN order_count_30d end order_count
		FROM dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
) ord
ON page.recent_days = ord.recent_days
JOIN
(
	SELECT  1 recent_days
	       ,COUNT(*) payment_count
	FROM dws_trade_user_payment_1d
	WHERE dt = '2020-06-14'
	UNION ALL
	SELECT  recent_days
	       ,SUM(if(order_count > 0,1,0))
	FROM
	(
		SELECT  recent_days
		       ,case recent_days WHEN 7 THEN payment_count_7d WHEN 30 THEN payment_count_30d end order_count
		FROM dws_trade_user_payment_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
) pay
ON page.recent_days = pay.recent_days;

DROP TABLE IF EXISTS ads_new_buyer_stats;
CREATE EXTERNAL TABLE ads_new_buyer_stats ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `new_order_user_count` BIGINT COMMENT '新增下单人数', `new_payment_user_count` BIGINT COMMENT '新增支付人数' ) COMMENT '新增交易用户统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_new_buyer_stats/';
INSERT OVERWRITE TABLE ads_new_buyer_stats
SELECT  *
FROM ads_new_buyer_stats
UNION
SELECT  '2020-06-14'
       ,odr.recent_days
       ,new_order_user_count
       ,new_payment_user_count
FROM
(
	SELECT  recent_days
	       ,SUM(if(order_date_first >= date_add('2020-06-14',-recent_days + 1),1,0)) new_order_user_count
	FROM dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp AS recent_days
	WHERE dt = '2020-06-14'
	GROUP BY  recent_days
) odr
JOIN
(
	SELECT  recent_days
	       ,SUM(if(payment_date_first >= date_add('2020-06-14',-recent_days + 1),1,0)) new_payment_user_count
	FROM dws_trade_user_payment_td lateral view explode(array(1, 7, 30)) tmp AS recent_days
	WHERE dt = '2020-06-14'
	GROUP BY  recent_days
) pay
ON odr.recent_days = pay.recent_days;

DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,7:最近7天,30:最近30天', `tm_id` STRING COMMENT '品牌ID', `tm_name` STRING COMMENT '品牌名称', `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率' ) COMMENT '各品牌复购率统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';
INSERT OVERWRITE TABLE ads_repeat_purchase_by_tm
SELECT  *
FROM ads_repeat_purchase_by_tm
UNION
SELECT  '2020-06-14' dt
       ,recent_days
       ,tm_id
       ,tm_name
       ,cast(SUM(if(order_count >= 2,1,0)) / SUM(if(order_count >= 1,1,0)) AS decimal(16,2))
FROM
(
	SELECT  '2020-06-14' dt
	       ,recent_days
	       ,user_id
	       ,tm_id
	       ,tm_name
	       ,SUM(order_count) order_count
	FROM
	(
		SELECT  recent_days
		       ,user_id
		       ,tm_id
		       ,tm_name
		       ,case recent_days WHEN 7 THEN order_count_7d WHEN 30 THEN order_count_30d end order_count
		FROM dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
	         ,user_id
	         ,tm_id
	         ,tm_name
) t2
GROUP BY  recent_days
         ,tm_id
         ,tm_name;

DROP TABLE IF EXISTS ads_trade_stats_by_tm;
CREATE EXTERNAL TABLE ads_trade_stats_by_tm ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `tm_id` STRING COMMENT '品牌ID', `tm_name` STRING COMMENT '品牌名称', `order_count` BIGINT COMMENT '订单数', `order_user_count` BIGINT COMMENT '订单人数', `order_refund_count` BIGINT COMMENT '退单数', `order_refund_user_count` BIGINT COMMENT '退单人数' ) COMMENT '各品牌商品交易统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_trade_stats_by_tm/';
INSERT OVERWRITE TABLE ads_trade_stats_by_tm
SELECT  *
FROM ads_trade_stats_by_tm
UNION
SELECT  '2020-06-14' dt
       ,nvl(odr.recent_days,refund.recent_days)
       ,nvl(odr.tm_id,refund.tm_id)
       ,nvl(odr.tm_name,refund.tm_name)
       ,nvl(order_count,0)
       ,nvl(order_user_count,0)
       ,nvl(order_refund_count,0)
       ,nvl(order_refund_user_count,0)
FROM
(
	SELECT  1 recent_days
	       ,tm_id
	       ,tm_name
	       ,SUM(order_count_1d) order_count
	       ,COUNT(distinct (user_id)) order_user_count
	FROM dws_trade_user_sku_order_1d
	WHERE dt = '2020-06-14'
	GROUP BY  tm_id
	         ,tm_name
	UNION ALL
	SELECT  recent_days
	       ,tm_id
	       ,tm_name
	       ,SUM(order_count)
	       ,COUNT(distinct (if(order_count > 0,user_id,null)))
	FROM
	(
		SELECT  recent_days
		       ,user_id
		       ,tm_id
		       ,tm_name
		       ,case recent_days WHEN 7 THEN order_count_7d WHEN 30 THEN order_count_30d end order_count
		FROM dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
	         ,tm_id
	         ,tm_name
) odr
FULL OUTER JOIN
(
	SELECT  1 recent_days
	       ,tm_id
	       ,tm_name
	       ,SUM(order_refund_count_1d) order_refund_count
	       ,COUNT(distinct (user_id)) order_refund_user_count
	FROM dws_trade_user_sku_order_refund_1d
	WHERE dt = '2020-06-14'
	GROUP BY  tm_id
	         ,tm_name
	UNION ALL
	SELECT  recent_days
	       ,tm_id
	       ,tm_name
	       ,SUM(order_refund_count)
	       ,COUNT(if(order_refund_count > 0,user_id,null))
	FROM
	(
		SELECT  recent_days
		       ,user_id
		       ,tm_id
		       ,tm_name
		       ,case recent_days WHEN 7 THEN order_refund_count_7d WHEN 30 THEN order_refund_count_30d end order_refund_count
		FROM dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
	         ,tm_id
	         ,tm_name
) refund
ON odr.recent_days = refund.recent_days AND odr.tm_id = refund.tm_id AND odr.tm_name = refund.tm_name;

DROP TABLE IF EXISTS ads_trade_stats_by_cate;
CREATE EXTERNAL TABLE ads_trade_stats_by_cate ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `category1_id` STRING COMMENT '一级分类id', `category1_name` STRING COMMENT '一级分类名称', `category2_id` STRING COMMENT '二级分类id', `category2_name` STRING COMMENT '二级分类名称', `category3_id` STRING COMMENT '三级分类id', `category3_name` STRING COMMENT '三级分类名称', `order_count` BIGINT COMMENT '订单数', `order_user_count` BIGINT COMMENT '订单人数', `order_refund_count` BIGINT COMMENT '退单数', `order_refund_user_count` BIGINT COMMENT '退单人数' ) COMMENT '各分类商品交易统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_trade_stats_by_cate/';
INSERT OVERWRITE TABLE ads_trade_stats_by_cate
SELECT  *
FROM ads_trade_stats_by_cate
UNION
SELECT  '2020-06-14' dt
       ,nvl(odr.recent_days,refund.recent_days)
       ,nvl(odr.category1_id,refund.category1_id)
       ,nvl(odr.category1_name,refund.category1_name)
       ,nvl(odr.category2_id,refund.category2_id)
       ,nvl(odr.category2_name,refund.category2_name)
       ,nvl(odr.category3_id,refund.category3_id)
       ,nvl(odr.category3_name,refund.category3_name)
       ,nvl(order_count,0)
       ,nvl(order_user_count,0)
       ,nvl(order_refund_count,0)
       ,nvl(order_refund_user_count,0)
FROM
(
	SELECT  1 recent_days
	       ,category1_id
	       ,category1_name
	       ,category2_id
	       ,category2_name
	       ,category3_id
	       ,category3_name
	       ,SUM(order_count_1d) order_count
	       ,COUNT(distinct (user_id)) order_user_count
	FROM dws_trade_user_sku_order_1d
	WHERE dt = '2020-06-14'
	GROUP BY  category1_id
	         ,category1_name
	         ,category2_id
	         ,category2_name
	         ,category3_id
	         ,category3_name
	UNION ALL
	SELECT  recent_days
	       ,category1_id
	       ,category1_name
	       ,category2_id
	       ,category2_name
	       ,category3_id
	       ,category3_name
	       ,SUM(order_count)
	       ,COUNT(distinct (if(order_count > 0,user_id,null)))
	FROM
	(
		SELECT  recent_days
		       ,user_id
		       ,category1_id
		       ,category1_name
		       ,category2_id
		       ,category2_name
		       ,category3_id
		       ,category3_name
		       ,case recent_days WHEN 7 THEN order_count_7d WHEN 30 THEN order_count_30d end order_count
		FROM dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
	         ,category1_id
	         ,category1_name
	         ,category2_id
	         ,category2_name
	         ,category3_id
	         ,category3_name
) odr
FULL OUTER JOIN
(
	SELECT  1 recent_days
	       ,category1_id
	       ,category1_name
	       ,category2_id
	       ,category2_name
	       ,category3_id
	       ,category3_name
	       ,SUM(order_refund_count_1d) order_refund_count
	       ,COUNT(distinct (user_id)) order_refund_user_count
	FROM dws_trade_user_sku_order_refund_1d
	WHERE dt = '2020-06-14'
	GROUP BY  category1_id
	         ,category1_name
	         ,category2_id
	         ,category2_name
	         ,category3_id
	         ,category3_name
	UNION ALL
	SELECT  recent_days
	       ,category1_id
	       ,category1_name
	       ,category2_id
	       ,category2_name
	       ,category3_id
	       ,category3_name
	       ,SUM(order_refund_count)
	       ,COUNT(distinct (if(order_refund_count > 0,user_id,null)))
	FROM
	(
		SELECT  recent_days
		       ,user_id
		       ,category1_id
		       ,category1_name
		       ,category2_id
		       ,category2_name
		       ,category3_id
		       ,category3_name
		       ,case recent_days WHEN 7 THEN order_refund_count_7d WHEN 30 THEN order_refund_count_30d end order_refund_count
		FROM dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
	         ,category1_id
	         ,category1_name
	         ,category2_id
	         ,category2_name
	         ,category3_id
	         ,category3_name
) refund
ON odr.recent_days = refund.recent_days AND odr.category1_id = refund.category1_id AND odr.category1_name = refund.category1_name AND odr.category2_id = refund.category2_id AND odr.category2_name = refund.category2_name AND odr.category3_id = refund.category3_id AND odr.category3_name = refund.category3_name;

DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate ( `dt` STRING COMMENT '统计日期', `category1_id` STRING COMMENT '一级分类ID', `category1_name` STRING COMMENT '一级分类名称', `category2_id` STRING COMMENT '二级分类ID', `category2_name` STRING COMMENT '二级分类名称', `category3_id` STRING COMMENT '三级分类ID', `category3_name` STRING COMMENT '三级分类名称', `sku_id` STRING COMMENT '商品id', `sku_name` STRING COMMENT '商品名称', `cart_num` BIGINT COMMENT '购物车中商品数量', `rk` BIGINT COMMENT '排名' ) COMMENT '各分类商品购物车存量Top10' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';
INSERT OVERWRITE TABLE ads_sku_cart_num_top3_by_cate
SELECT  *
FROM ads_sku_cart_num_top3_by_cate
UNION
SELECT  '2020-06-14' dt
       ,category1_id
       ,category1_name
       ,category2_id
       ,category2_name
       ,category3_id
       ,category3_name
       ,sku_id
       ,sku_name
       ,cart_num
       ,rk
FROM
(
	SELECT  sku_id
	       ,sku_name
	       ,category1_id
	       ,category1_name
	       ,category2_id
	       ,category2_name
	       ,category3_id
	       ,category3_name
	       ,cart_num
	       ,rank() over (partition by category1_id,category2_id,category3_id ORDER BY cart_num desc) rk
	FROM
	(
		SELECT  sku_id
		       ,SUM(sku_num) cart_num
		FROM dwd_trade_cart_full
		WHERE dt = '2020-06-14'
		GROUP BY  sku_id
	) cart
	LEFT JOIN
	(
		SELECT  id
		       ,sku_name
		       ,category1_id
		       ,category1_name
		       ,category2_id
		       ,category2_name
		       ,category3_id
		       ,category3_name
		FROM dim_sku_full
		WHERE dt = '2020-06-14'
	) sku
	ON cart.sku_id = sku.id
) t1
WHERE rk <= 3;

DROP TABLE IF EXISTS ads_trade_stats;
CREATE EXTERNAL TABLE ads_trade_stats ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1日,7:最近7天,30:最近30天', `order_total_amount` DECIMAL(16, 2) COMMENT '订单总额,GMV', `order_count` BIGINT COMMENT '订单数', `order_user_count` BIGINT COMMENT '下单人数', `order_refund_count` BIGINT COMMENT '退单数', `order_refund_user_count` BIGINT COMMENT '退单人数' ) COMMENT '交易统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_trade_stats/';
INSERT OVERWRITE TABLE ads_trade_stats
SELECT  *
FROM ads_trade_stats
UNION
SELECT  '2020-06-14'
       ,odr.recent_days
       ,order_total_amount
       ,order_count
       ,order_user_count
       ,order_refund_count
       ,order_refund_user_count
FROM
(
	SELECT  1 recent_days
	       ,SUM(order_total_amount_1d) order_total_amount
	       ,SUM(order_count_1d) order_count
	       ,COUNT(*) order_user_count
	FROM dws_trade_user_order_1d
	WHERE dt = '2020-06-14'
	UNION ALL
	SELECT  recent_days
	       ,SUM(order_total_amount)
	       ,SUM(order_count)
	       ,SUM(if(order_count > 0,1,0))
	FROM
	(
		SELECT  recent_days
		       ,case recent_days WHEN 7 THEN order_total_amount_7d WHEN 30 THEN order_total_amount_30d end order_total_amount
		       ,case recent_days WHEN 7 THEN order_count_7d WHEN 30 THEN order_count_30d end order_count
		FROM dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
) odr
JOIN
(
	SELECT  1 recent_days
	       ,SUM(order_refund_count_1d) order_refund_count
	       ,COUNT(*) order_refund_user_count
	FROM dws_trade_user_order_refund_1d
	WHERE dt = '2020-06-14'
	UNION ALL
	SELECT  recent_days
	       ,SUM(order_refund_count)
	       ,SUM(if(order_refund_count > 0,1,0))
	FROM
	(
		SELECT  recent_days
		       ,case recent_days WHEN 7 THEN order_refund_count_7d WHEN 30 THEN order_refund_count_30d end order_refund_count
		FROM dws_trade_user_order_refund_nd lateral view explode(array(7, 30)) tmp AS recent_days
		WHERE dt = '2020-06-14'
	) t1
	GROUP BY  recent_days
) refund
ON odr.recent_days = refund.recent_days;

DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE ads_order_by_province ( `dt` STRING COMMENT '统计日期', `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天', `province_id` STRING COMMENT '省份ID', `province_name` STRING COMMENT '省份名称', `area_code` STRING COMMENT '地区编码', `iso_code` STRING COMMENT '国际标准地区编码', `iso_code_3166_2` STRING COMMENT '国际标准地区编码', `order_count` BIGINT COMMENT '订单数', `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额' ) COMMENT '各地区订单统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_order_by_province/';
INSERT OVERWRITE TABLE ads_order_by_province
SELECT  *
FROM ads_order_by_province
UNION
SELECT  '2020-06-14' dt
       ,1 recent_days
       ,province_id
       ,province_name
       ,area_code
       ,iso_code
       ,iso_3166_2
       ,order_count_1d
       ,order_total_amount_1d
FROM dws_trade_province_order_1d
WHERE dt = '2020-06-14'
UNION
SELECT  '2020-06-14' dt
       ,recent_days
       ,province_id
       ,province_name
       ,area_code
       ,iso_code
       ,iso_3166_2
       ,SUM(order_count)
       ,SUM(order_total_amount)
FROM
(
	SELECT  recent_days
	       ,province_id
	       ,province_name
	       ,area_code
	       ,iso_code
	       ,iso_3166_2
	       ,case recent_days WHEN 7 THEN order_count_7d WHEN 30 THEN order_count_30d end order_count
	       ,case recent_days WHEN 7 THEN order_total_amount_7d WHEN 30 THEN order_total_amount_30d end order_total_amount
	FROM dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp AS recent_days
	WHERE dt = '2020-06-14'
) t1
GROUP BY  recent_days
         ,province_id
         ,province_name
         ,area_code
         ,iso_code
         ,iso_3166_2;

DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats ( `dt` STRING COMMENT '统计日期', `coupon_id` STRING COMMENT '优惠券ID', `coupon_name` STRING COMMENT '优惠券名称', `start_date` STRING COMMENT '发布日期', `rule_name` STRING COMMENT '优惠规则，例如满100元减10元', `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率' ) COMMENT '优惠券统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';
INSERT OVERWRITE TABLE ads_coupon_stats
SELECT  *
FROM ads_coupon_stats
UNION
SELECT  '2020-06-14' dt
       ,coupon_id
       ,coupon_name
       ,start_date
       ,coupon_rule
       ,cast(coupon_reduce_amount_30d / original_amount_30d AS decimal(16,2))
FROM dws_trade_coupon_order_nd
WHERE dt = '2020-06-14';

DROP TABLE IF EXISTS ads_activity_stats;
CREATE EXTERNAL TABLE ads_activity_stats ( `dt` STRING COMMENT '统计日期', `activity_id` STRING COMMENT '活动ID', `activity_name` STRING COMMENT '活动名称', `start_date` STRING COMMENT '活动开始日期', `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率' ) COMMENT '活动统计' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/warehouse/gmall/ads/ads_activity_stats/';
INSERT OVERWRITE TABLE ads_activity_stats
SELECT  *
FROM ads_activity_stats
UNION
SELECT  '2020-06-14' dt
       ,activity_id
       ,activity_name
       ,start_date
       ,cast(activity_reduce_amount_30d / original_amount_30d AS decimal(16,2))
FROM dws_trade_activity_order_nd
WHERE dt = '2020-06-14';