-- 交易域 用户商品粒度订单最近1日汇总表
select * from dws_trade_user_sku_order_1d order by user_id;
-- 交易域用户商品粒度退单最近1日汇总表
select * from dws_trade_user_sku_order_refund_1d order by user_id;
-- 交易域用户粒度订单最近1日汇总表
select * from dws_trade_user_order_1d order by user_id;
-- 交易域用户粒度加购最近1日汇总表
select * from dws_trade_user_cart_add_1d order by user_id;
-- 交易域用户粒度支付最近1日汇总事实表
select * from dws_trade_user_payment_1d order by user_id;
-- 交易域省份粒度订单最近1日汇总事实表
select * from dws_trade_province_order_1d order by province_id;
-- 交易域用户粒度退单最近1日汇总事实表
select * from dws_trade_user_order_refund_1d order by user_id;


-- 流量域 会话粒度 页面浏览 最近1日汇总表
select * from dws_traffic_session_page_view_1d order by session_id;
-- 流量域 访客页面粒度 页面浏览 最近1日汇总事实表
select * from dws_traffic_page_visitor_page_view_1d order by page_id;

-- 交易域 用户商品粒度 订单 最近n日汇总 事实表
select * from dws_trade_user_sku_order_nd order by dt,  user_id ;
-- 交易域 用户商品粒度 退单 最近n日汇总 事实表
select * from dws_trade_user_sku_order_refund_nd;
-- 交易域 用户粒度 订单最近n日汇总 事实表
select * from dws_trade_user_order_nd order by dt, user_id;
-- 交易域 用户粒度 加购 最近n日汇总 事实表
select * from dws_trade_user_cart_add_nd order by dt, user_id;
-- 交易域 用户粒度 支付 最近n日汇总 事实表
select * from dws_trade_user_payment_nd order by dt, user_id;
-- 交易域 省份粒度 订单 最近n日汇总 事实表
select * from dws_trade_province_order_nd order by dt, province_id;
-- 交易域 优惠券粒度 订单 最近n日汇总 事实表
select * from dws_trade_coupon_order_nd;
-- 交易域 活动粒度 订单 最近n日汇总 事实表
select * from dws_trade_activity_order_nd;
-- 交易域 用户粒度 退单 最近n日汇总 事实表
select * from dws_trade_user_order_refund_nd order by dt, user_id;

-- 流量域 访客页面粒度 页面浏览 最近n日汇总 事实表
select * from dws_traffic_page_visitor_page_view_nd order by dt, page_id;

-- 交易域 用户粒度 订单历史至今 汇总 事实表
select * from dws_trade_user_order_td order by user_id;
-- 交易域 用户粒度 支付历史至今 汇总 事实表
select * from dws_trade_user_payment_td order by user_id;
-- 用户域 用户粒度 登录历史至今 汇总事实表
select * from dws_user_user_login_td order by user_id;
