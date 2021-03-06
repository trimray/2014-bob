require 'active_record'
require 'mysql2'
require 'yaml'

config = YAML::load(File.open(File.dirname(__FILE__) + "/database.yml"))
ActiveRecord::Base.establish_connection(config)

if ARGV.include?('--debug')
  # dao = ActiveRecord::Base.connection
  ShopArea = Class.new ActiveRecord::Base
  ShopArea.where(active:'1').find_each do |tar|
    level = ShopArea.where(['lft <= ? AND rgt >=?', tar.lft, tar.rgt]).count
    son_num = (tar.rgt.to_i - tar.lft.to_i - 1)/2
    p tar.area_name if level == 2 && son_num == 0
  end
  exit
end

def self.perform
  p "=== task starts at #{print_time} ==="
  import_shop_areas
  import_shop_user
  import_shop_member
  import_shop_order
  import_shop_order_goods
  import_shop_coupon
  delete_shop_refundment_doc
  import_shop_collection_doc
  import_shop_comment
  p "=== task ends at #{print_time} ==="
end

def import_shop_user
  p ">> DELETE emall.shop_user"
  dao.execute( 'TRUNCATE emall.shop_user;' )
  print_finish

  # emall.shop_user要alter字段
    # 1.password字段 增加长度
    # 2.password_salt字段 增加字段
    # 3.username字段 增加长度
    # 4.username字段 改索引(UNIQUE -> INDEX)
    # 5.email字段 改索引(INDEX -> UNIQUE)
  p ">> ALTER emall.shop_user"
    alter_sql = 'ALTER TABLE `emall`.`shop_user` ' + \
      'CHANGE COLUMN `password` `password` VARCHAR(128) NOT NULL COMMENT "密码",' + \
      'ADD COLUMN `password_salt` VARCHAR(255) NOT NULL COMMENT "密码盐" AFTER `password`,' + \
      'CHANGE COLUMN `username` `username` VARCHAR(255) NOT NULL COMMENT "用户名",' + \
      'DROP INDEX `username`, ADD INDEX `username` (`username` ASC),' + \
      'DROP INDEX `email`, ADD UNIQUE INDEX `email` (`email` ASC);'
    dao.execute(alter_sql)
  print_finish

  p ">> INSERT emall.shop_user"
    total = dao.select_value( 'select max(id) from ruby.users;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_user (id, username, password, password_salt, email)' + \
        "select id, username, encrypted_password, password_salt, email from ruby.users where id > #{start_id} and id <= #{1000 + start_id};"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
  print_finish
end

def import_shop_member
  p ">> DELETE emall.shop_member"
  dao.execute( 'TRUNCATE emall.shop_member;' )
  print_finish

  # emall.shop_member 要alter字段
    # 1.qq字段 增加长度
    # 2.sex字段 修改备注
    # 3.history字段 增加字段
  p ">> ALTER emall.shop_member"
    alter_sql = 'ALTER TABLE `emall`.`shop_member`' +' '+\
      'CHANGE COLUMN `qq` `qq` VARCHAR(255) NULL DEFAULT NULL COMMENT "QQ",' +' '+\
      'ADD COLUMN `history` VARCHAR(255) NULL DEFAULT NULL AFTER `custom`,' +' '+\
      'CHANGE COLUMN `sex` `sex` TINYINT(1) NOT NULL DEFAULT "3" COMMENT "性别1男2女3保密" ;'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_member 21列
    # INSERT 15列
    # 废弃 exp, message_ids, prop, balance, custom
  p ">> INSERT emall.shop_member"
    total = dao.select_value( 'select max(id) from ruby.users;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_member' +' '+\
        '(user_id, true_name, telephone, mobile, contact_addr, qq, msn, sex, birthday, group_id, point, zip, status, time, last_login)' +' '+\
        'select id, name, telephone, mobile, address, qq, msn, gender, birthday, 4, integral, zipcode, 1,' +' '+\
        "#{ sql_time_transfer('created_at')}, #{ sql_time_transfer('last_sign_in_at')}" +' '+\
        "from ruby.users where id > #{start_id} and id <= #{1000 + start_id};"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
    # todo TANSFER area
    # TANSFER group_id
    staff_ids = dao.select_values( 'select id from ruby.users where user_type = "staff"; ' )
    staff_ids.each_slice(1000) do |play_ids|
      dao.execute( "update emall.shop_member set group_id = 5 where user_id in (#{ play_ids.join(',') });" )
    end
    partner_ids = dao.select_values( 'select id from ruby.users where user_type = "partner"; ' )
    partner_ids.each_slice(1000) do |play_ids|
      dao.execute( "update emall.shop_member set group_id = 6 where user_id in (#{ play_ids.join(',') });" )
    end
    # TANSFER status
    status_ids = dao.select_values( 'select id from ruby.users where blocked_at is not null;' )
    status_ids.each_slice(1000) do |play_ids|
      dao.execute( "update emall.shop_member set status = 3 where user_id in (#{ play_ids.join(',') });" )
    end
  print_finish
end

def import_shop_order
  p ">> DELETE emall.shop_order"
  dao.execute( 'TRUNCATE emall.shop_order;' )
  print_finish

  # emall.shop_order 要alter字段
    # 1.order_type字段 增加长度
    # 2.status字段 修改默认值 修改备注
    # 3.accept_time字段 删除1增加4 [cancel_time, request_refundment_time, affirm_refundment_time, complete_refundment_time, updated_at]
    # 4.province city area字段 修改
  p ">> ALTER emall.shop_order"
    alter_sql = 'ALTER TABLE `emall`.`shop_order`' +' '+\
      'CHANGE COLUMN `status` `status` TINYINT(1) NULL DEFAULT "0" COMMENT "订单状态:' +' '+\
        '0等待买家付款, 1买家已付款, 2卖家已发货, 3交易成功, 8退款中, 9退款确认, 10已退款, -10交易取消",' +' '+\
      'DROP COLUMN `accept_time`,' +' '+\
      'CHANGE COLUMN `order_type` `order_type` VARCHAR(255) NOT NULL COMMENT "订单类型",' +' '+\
      'ADD COLUMN `cancel_time` datetime DEFAULT NULL AFTER `real_freight`,' +' '+\
      'ADD COLUMN `request_refundment_time` datetime DEFAULT NULL AFTER `cancel_time`,' +' '+\
      'ADD COLUMN `affirm_refundment_time` datetime DEFAULT NULL AFTER `request_refundment_time`,' +' '+\
      'ADD COLUMN `complete_refundment_time` datetime DEFAULT NULL AFTER `affirm_refundment_time`,' +' '+\
      'ADD COLUMN `updated_at` datetime DEFAULT NULL AFTER `complete_refundment_time`,' +' '+\
      'CHANGE COLUMN `distribution` `distribution` VARCHAR(255) NULL DEFAULT NULL COMMENT \'ERP返回的配送信息\',' +' '+\
      'CHANGE COLUMN `province` `province` VARCHAR(255) DEFAULT NULL COMMENT \'省\',' +' '+\
      'CHANGE COLUMN `city` `city` VARCHAR(255) DEFAULT NULL COMMENT \'市\',' +' '+\
      'CHANGE COLUMN `area` `area` VARCHAR(255) DEFAULT NULL COMMENT \'区\';'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_order 44列
    # INSERT 31列
    # order_no字段 存salt
    # 废弃 country, distribution, pay_status, distribution_status, if_del, insured, if_insured, pay_fee, taxes, discount, if_print, prop, exp, type
  p ">> INSERT emall.shop_order"
    total = dao.select_value( 'select max(id) from ruby.orders;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_order' +' '+\
        '(id, order_no, user_id, pay_type, status, accept_name, postcode, telphone, address, mobile,' +' '+\
        '  payable_amount, real_amount, payable_freight, real_freight, province, city, area,' +' '+\
        '  cancel_time, request_refundment_time, affirm_refundment_time, complete_refundment_time, create_time, pay_time, send_time, completion_time, updated_at,' +' '+\
        '  invoice, postscript, note, invoice_title, order_amount, point, order_type)' +' '+\
        'select id, salt, user_id, 5, state, shipping_contact_name, shipping_zipcode, shipping_telephone, shipping_address, shipping_mobile,' +' '+\
        '  items_total_price, items_total_price, shipping_fee, shipping_fee, shipping_state, shipping_city, shipping_district,' +' '+\
        "  #{ sql_time_transfer('cancel_time')}, #{ sql_time_transfer('request_refundment_time')}, #{ sql_time_transfer('affirm_refundment_time')}," +' '+\
        "  #{ sql_time_transfer('complete_refundment_time')}, #{ sql_time_transfer('created_at')}, #{ sql_time_transfer('deal_time')}," +' '+\
        "  #{ sql_time_transfer('send_goods_at')}, #{ sql_time_transfer('complete_time')}, #{ sql_time_transfer('updated_at')}," +' '+\
        '  1+COALESCE(`invoice_type`, -1), buyer_order_message, seller_memo, invoice_title, total_price, integral, order_type' +' '+\
        "from ruby.orders where id > #{start_id} and id <= #{1000 + start_id} AND items_total_price IS NOT null;"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
  print_finish
end

def import_shop_order_goods
  p ">> DELETE emall.shop_order_goods"
  dao.execute( 'TRUNCATE emall.shop_order_goods;' )
  print_finish

  # emall.shop_order_goods要alter字段
    # 1.img字段 可以为NULL
    # 2.color_content字段 增加字段
  p ">> ALTER emall.shop_order_goods"
    alter_sql = 'ALTER TABLE `emall`.`shop_order_goods` '  +' '+\
      'CHANGE COLUMN `img` `img` VARCHAR(255) NULL COMMENT "商品图片" ,'  +' '+\
      'ADD COLUMN `color_content` VARCHAR(255) NOT NULL AFTER `color`;'
    dao.execute(alter_sql)
  print_finish

  # 废弃 product_id, goods_weight
  p ">> INSERT emall.shop_order_goods"
    total = dao.select_value( 'select max(id) from ruby.order_items;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_order_goods (id, order_id, goods_id, goods_price, real_price, goods_nums, color, color_content)' + \
        'select id, order_id, product_id, unit_price, unit_price, quantity, swap_colors, swap_colors_content from ruby.order_items'  +' '+\
        "where id > #{start_id} and id <= #{1000 + start_id};"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
    # todo TANSFER img
    # todo TANSFER goods_array
  print_finish
end

def import_shop_coupon
  p ">> DELETE emall.shop_coupon_number"
  dao.execute( 'TRUNCATE emall.shop_coupon_number;' )
  print_finish

  p ">> DELETE emall.shop_coupon"
  dao.execute( 'TRUNCATE emall.shop_coupon;' )
  print_finish

  #emall.shop_coupon要alter字段
  # 1.obtain_reason 增加字段
  # 2.old_string 增加字段
  # 3.reg_obtain 增加字段
  p ">> ALTER emall.shop_coupon"
    alter_sql = 'ALTER TABLE `emall`.`shop_coupon`' +' '+\
      'ADD COLUMN `obtain_reason` VARCHAR(255) NOT NULL COMMENT "优惠券发放原因",' +' '+\
      'ADD COLUMN `old_string` VARCHAR(255) NOT NULL COMMENT "老优惠券coupon_string",' +' '+\
      'ADD COLUMN `reg_obtain` bit(1) NOT NULL DEFAULT b\'0\' COMMENT "注册自动获得:1获得";'
    dao.execute(alter_sql)
  print_finish

  #emall.shop_coupon_number要alter字段
  # 1.order_id字段 增加字段
  p ">> ALTER emall.shop_coupon_number"
    alter_sql = 'ALTER TABLE `emall`.`shop_coupon_number`' +' '+\
      'ADD COLUMN `order_id` int(11) DEFAULT \'0\',' +' '+\
      'ADD INDEX `order_id` (`order_id` ASC) COMMENT "订单号" ;'
    dao.execute(alter_sql)
  print_finish

  p ">> INSERT emall.shop_coupon"
    insert_sql = 'insert into emall.shop_coupon (old_string, obtain_reason, name, rule, money, isActivity, startTime, endTime, updateTime)' +' '+\
      'select coupon_string, obtain_reason, coupon_name, limit_price, discount_fee, 1,' +' '+\
      "  #{ sql_time_transfer('max(coupon_start)', 'coupon_start')}, #{ sql_time_transfer('max(coupon_end)', 'coupon_end')}," +' '+\
      "  #{ sql_time_transfer('min(updated_at)', 'updated_at')}" +' '+\
      'from ruby.coupons where coupon_string IS NOT null group by coupon_string order by max(coupon_start)'
    dao.execute(insert_sql)
  print_finish

  p ">> INSERT emall.shop_coupon_number"
    c_names = dao.select_rows( 'select id, old_string from emall.shop_coupon;' )
    c_names.each do |row|
      c_id = row.first
      c_name = row.second
      p "   import c_id=#{c_id}"
      insert_sql = 'insert into emall.shop_coupon_number (id, number, cid, userid, money, status, isUse, order_id, startTime, endTime, createTime)' + \
        "select id, coupon_number, #{c_id}, user_id, discount_fee, 1, COALESCE(`is_used`, 1), COALESCE(`order_id`, 0)," +' '+\
        "  #{ sql_time_transfer('coupon_start')}, #{ sql_time_transfer('coupon_end')}, #{ sql_time_transfer('created_at')}" +' '+\
        "from ruby.coupons where coupon_string='#{c_name}' AND user_id IS NOT null"
      dao.execute(insert_sql)
    end
  print_finish
end

def import_shop_areas
  p ">> DELETE emall.shop_areas"
  dao.execute( 'TRUNCATE emall.shop_areas;' )
  print_finish

  #emall.shop_areas要alter字段
  # 1.lft字段 增加字段
  # 2.rgt字段 增加字段
  # 3.active字段 增加字段
  p ">> ALTER emall.shop_areas"
    alter_sql = 'ALTER TABLE `emall`.`shop_areas`' +' '+\
      'ADD COLUMN `lft` INT(10) NOT NULL COMMENT "左值" AFTER `parent_id`,' +' '+\
      'ADD COLUMN `rgt` INT(10) NOT NULL COMMENT "右值" AFTER `lft`,' +' '+\
      'ADD COLUMN `active` TINYINT(1) NOT NULL DEFAULT 0 COMMENT "是否使用" AFTER `area_name`;'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_order 7列
    # INSERT 6列
    # 废弃 sort
  p ">> INSERT emall.shop_areas"
    insert_sql = 'insert into emall.shop_areas (area_id, parent_id, lft, rgt, area_name, active)' +' '+\
      'select id, COALESCE(`parent_id`, 0), lft, rgt, name, active from ruby.areas'
    dao.execute(insert_sql)
  print_finish
end

def delete_shop_refundment_doc
  p ">> DELETE emall.shop_refundment_doc"
  dao.execute( 'TRUNCATE emall.shop_refundment_doc;' )
  print_finish
end

def import_shop_collection_doc
  p ">> DELETE emall.shop_collection_doc"
  dao.execute( 'TRUNCATE emall.shop_collection_doc;' )
  print_finish

  # emall.shop_collection_doc 要alter字段
    # 1.user_id字段 可为空
    # 2.增加支付宝的参数字段 18个
  p ">> ALTER emall.shop_collection_doc"
    alter_sql = 'ALTER TABLE `emall`.`shop_collection_doc`' +' '+\
      'CHANGE COLUMN `user_id` `user_id` INT(11) UNSIGNED NULL COMMENT "用户ID",' +' '+\
      'ADD COLUMN `ali_body` VARCHAR(255) NULL DEFAULT NULL AFTER `user_id`,' +' '+\
      'ADD COLUMN `ali_buyer_email` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_body`,' +' '+\
      'ADD COLUMN `ali_buyer_id` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_buyer_email`,' +' '+\
      'ADD COLUMN `ali_exterface` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_buyer_id`,' +' '+\
      'ADD COLUMN `ali_is_success` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_exterface`,' +' '+\
      'ADD COLUMN `ali_notify_id` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_is_success`,' +' '+\
      'ADD COLUMN `ali_notify_time` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_notify_id`,' +' '+\
      'ADD COLUMN `ali_notify_type` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_notify_time`,' +' '+\
      'ADD COLUMN `ali_out_trade_no` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_notify_type`,' +' '+\
      'ADD COLUMN `ali_payment_type` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_out_trade_no`,' +' '+\
      'ADD COLUMN `ali_seller_email` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_payment_type`,' +' '+\
      'ADD COLUMN `ali_seller_id` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_seller_email`,' +' '+\
      'ADD COLUMN `ali_subject` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_seller_id`,' +' '+\
      'ADD COLUMN `ali_total_fee` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_subject`,' +' '+\
      'ADD COLUMN `ali_trade_no` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_total_fee`,' +' '+\
      'ADD COLUMN `ali_trade_status` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_trade_no`,' +' '+\
      'ADD COLUMN `ali_sign` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_trade_status`,' +' '+\
      'ADD COLUMN `ali_sign_type` VARCHAR(255) NULL DEFAULT NULL AFTER `ali_sign`;'
    dao.execute(alter_sql)
  print_finish

  p ">> INSERT emall.shop_collection_doc"
    total = dao.select_value( 'select max(id) from ruby.alipay_notifies;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_collection_doc' +' '+\
        '(id, order_id, amount, time, payment_id, pay_status, if_del,' +' '+\
        'ali_body, ali_buyer_email, ali_buyer_id, ali_exterface, ali_is_success, ali_notify_id, ali_notify_time, ali_notify_type, ali_out_trade_no,' +' '+\
        'ali_payment_type, ali_seller_email, ali_seller_id, ali_subject, ali_total_fee, ali_trade_no, ali_trade_status, ali_sign, ali_sign_type)' +' '+\
        "select id, order_id, total_fee, #{sql_time_transfer('notify_time')}, 5, 1, 0,"+' '+\
        'body, buyer_email, buyer_id, exterface, is_success, notify_id, notify_time, notify_type, out_trade_no,' +' '+\
        'payment_type, seller_email, seller_id, subject, total_fee, trade_no, trade_status, sign, sign_type' +' '+\
        "from ruby.alipay_notifies where id > #{start_id} and id <= #{1000 + start_id} AND trade_status = 'TRADE_SUCCESS' ;"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
  print_finish
end

def import_shop_comment
  p ">> DELETE emall.shop_comment"
  dao.execute( 'TRUNCATE emall.shop_comment;' )
  print_finish

  #emall.shop_comment要alter字段
  # 1.disable字段 冗余status字段 删除
  # 2.status字段 修改备注
  p ">> ALTER emall.shop_comment"
    alter_sql = 'ALTER TABLE `emall`.`shop_comment`' +' '+\
      'DROP COLUMN `disable`,' +' '+\
      "CHANGE COLUMN `status` `status` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '评论状态：0：隐藏的评论 1:正常的评论'"
    dao.execute(alter_sql)
  print_finish

  # emall.shop_comment 11列
    # INSERT 10列
    # 废弃 point disable
  p ">> INSERT emall.shop_comment"
    insert_sql = 'insert into emall.shop_comment (id, user_id, goods_id, order_no, contents, status, is_like, time, comment_time)' +' '+\
      'select c.id, c.user_id, c.product_id, o.salt, c.content, abs(c.disabled-1), c.is_like,' +' '+\
      "  #{sql_time_transfer('o.created_at', 'order_created_at')}, #{sql_time_transfer('c.created_at', 'comment_created_at')}" +' '+\
      'from  ruby.comments as c left join ruby.order_items as i on c.order_item_id = i.id left join ruby.orders as o on i.order_id = o.id'
    dao.execute(insert_sql)
  print_finish
end

private
def print_time
  Time.now.strftime("%F %T")
end

def print_finish
  p '>> FINISHED at ' + print_time
end

def sql_time_transfer(field_name, as_field_name = field_name)
  "DATE_ADD(#{field_name},INTERVAL 8 HOUR) AS #{as_field_name}"
end

class Tracer
  attr_reader :dig
  def initialize; @dig = ActiveRecord::Base.connection end
  def execute(s); p '## EXECUTE >>' + s; dig.execute s end
  def select_value(s); p '## QUERY >>' + s; dig.select_value s end
  def select_values(s); p '## QUERY >>' + s; dig.select_values s end
  def select_rows(s); p '## QUERY >>' + s; dig.select_rows s end
end

def dao
  if ARGV.include?('--trace')
    Tracer.new
  else
    ActiveRecord::Base.connection
  end
end

self.perform