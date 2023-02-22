-- 各渠道流量统计
SELECT  *
FROM ads_traffic_stats_by_channel order by channel;

-- 页面浏览路径分析
SELECT  *
FROM ads_page_path;

-- 用户变动统计
SELECT  *
FROM ads_user_change;

-- 用户留存率
SELECT  *
FROM ads_user_retention;

-- 用户新增活跃统计
SELECT  *
FROM ads_user_stats order by dt, recent_days;

-- 漏斗分析
SELECT  *
FROM ads_user_action  order by dt, recent_days;

-- 新增交易用户统计
SELECT  *
FROM ads_new_buyer_stats order by dt, recent_days;

-- 各品牌复购率统计
SELECT  *
FROM ads_repeat_purchase_by_tm order by dt, tm_id;

-- 各品牌商品交易统计
SELECT  *
FROM ads_trade_stats_by_tm order by dt, tm_id, recent_days;

-- 各分类商品交易统计
SELECT  *
FROM ads_trade_stats_by_cate order by dt, category1_id, category2_id, category3_id, recent_days;

-- 各分类商品购物车存量Top10
SELECT  *
FROM ads_sku_cart_num_top3_by_cate order by dt, category1_id, category2_id, category3_id, rk;

-- 交易统计
SELECT  *
FROM ads_trade_stats order by dt, recent_days;

-- 各地区订单统计
SELECT  *
FROM ads_order_by_province order by dt, province_id, recent_days;

-- 优惠券统计
SELECT  *
FROM ads_coupon_stats order by dt, coupon_id;

-- 活动统计
SELECT  *
FROM ads_activity_stats order by dt, activity_id;