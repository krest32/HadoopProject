-- 交易域加购事务事实表
select * from dwd_trade_cart_add_inc;
-- 交易域下单事务事实表
select * from dwd_trade_order_detail_inc;
-- 交易域取消订单事务事实表
select * from dwd_trade_cancel_detail_inc;
-- 交易域支付成功事务事实表
select * from dwd_trade_pay_detail_suc_inc;
-- 交易域退单事务事实表
select * from dwd_trade_order_refund_inc;
-- 交易域退款成功事务事实表
select * from dwd_trade_refund_pay_suc_inc;
-- 交易域购物车周期快照事实表
select * from dwd_trade_cart_full order by user_id;
-- 工具域优惠券领取事务事实表
select * from dwd_tool_coupon_get_inc;
-- 工具域优惠券使用(下单)事务事实表
select * from dwd_tool_coupon_order_inc;
-- 工具域优惠券使用(支付)事务事实表
select * from dwd_tool_coupon_pay_inc;
-- 互动域收藏商品事务事实表
select * from dwd_interaction_favor_add_inc;
-- 互动域评价事务事实表
select * from dwd_interaction_comment_inc;
-- 流量域页面浏览事务事实表
select * from dwd_traffic_page_view_inc order by mid_id;
-- 流量域启动事务事实表
select * from dwd_traffic_start_inc order by user_id;
-- 流量域动作事务事实表
select * from dwd_traffic_action_inc order by user_id;
-- 流量域曝光事务事实表
select * from dwd_traffic_display_inc order by user_id;
-- 流量域错误事务事实表
select * from dwd_traffic_error_inc order by mid_id;
-- 用户域用户注册事务事实表
select * from dwd_user_register_inc;
-- 用户域用户登录事务事实表
select * from dwd_user_login_inc order by user_id;