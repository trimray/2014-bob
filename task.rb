require 'active_record'
require 'mysql2'
require 'yaml'

config = YAML::load(File.open(File.dirname(__FILE__) + "/database.yml"))
ActiveRecord::Base.establish_connection(config)

if ARGV.include?('-d')
  # require File.join(File.dirname(__FILE__),'shop_order')
  dao = ActiveRecord::Base.connection
  def print_finish; p( '>> DEBUG at ' + Time.now.strftime("%F %T")) end

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
    total = dao.select_value( 'select count(*) from ruby.users;' )
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
  p ">> ALTER emall.shop_member"
    alter_sql = 'ALTER TABLE `emall`.`shop_member`' +' '+\
      'CHANGE COLUMN `qq` `qq` VARCHAR(255) NULL DEFAULT NULL COMMENT "QQ",' +' '+\
      'CHANGE COLUMN `sex` `sex` TINYINT(1) NOT NULL DEFAULT "3" COMMENT "性别1男2女3保密" ;'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_member 21列
    # INSERT 15列
    # 废弃 exp, message_ids, prop, balance, custom
  p ">> INSERT emall.shop_member"
    total = dao.select_value( 'select count(*) from ruby.users;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_member' +' '+\
        '(user_id, true_name, telephone, mobile, contact_addr, qq, msn, sex, birthday, group_id, point, time, zip, status, last_login)' +' '+\
        'select id, name, telephone, mobile, address, qq, msn, gender, birthday, 4, integral, created_at, zipcode, 1, last_sign_in_at' +' '+\
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
  p ">> ALTER emall.shop_order"
    alter_sql = 'ALTER TABLE `emall`.`shop_order`' +' '+\
      'CHANGE COLUMN `order_type` `order_type` VARCHAR(255) NOT NULL COMMENT "订单类型";'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_order 44列
    # INSERT 31列
    # order_no字段 存salt
    # 废弃 distribution, pay_status, distribution_status, if_del, insured, if_insured, pay_fee, taxes, discount, if_print, prop, exp, type
  p ">> INSERT emall.shop_order"
    total = dao.select_value( 'select count(*) from ruby.orders;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_order' +' '+\
        '(id, order_no, user_id, pay_type, status, accept_name, postcode, telphone, address, mobile,' +' '+\
        '  payable_amount, real_amount, payable_freight, real_freight, pay_time, send_time, create_time, completion_time,' +' '+\
        '  invoice, postscript, note, invoice_title, order_amount, accept_time, point, order_type)' +' '+\
        'select id, salt, user_id, 5, state, shipping_contact_name, shipping_zipcode, shipping_telephone, shipping_address, shipping_mobile,' +' '+\
        '  items_total_price, items_total_price, shipping_fee, shipping_fee, deal_time, send_goods_at, created_at, complete_time,' +' '+\
        '  1+COALESCE(`invoice_type`, -1), buyer_order_message, seller_memo, invoice_title, total_price, complete_time, integral, order_type' +' '+\
        "from ruby.orders where id > #{start_id} and id <= #{1000 + start_id} AND items_total_price IS NOT null;"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
    # todo TANSFER country
    # todo TANSFER province
    # todo TANSFER city
    # todo TANSFER area
    # todo TANSFER promotions => Order::discount_fee
  print_finish
end

def import_shop_order_goods
  p ">> DELETE emall.shop_order_goods"
  dao.execute( 'TRUNCATE emall.shop_order_goods;' )
  print_finish

  # emall.shop_order_goods要alter字段
    # 1.img字段 可以为NULL
  p ">> ALTER emall.shop_order_goods"
    alter_sql = 'ALTER TABLE `emall`.`shop_order_goods` '  +' '+\
      'CHANGE COLUMN `img` `img` VARCHAR(255) NULL COMMENT "商品图片" ;'
    dao.execute(alter_sql)
  print_finish

  # 废弃 product_id, goods_weight
  p ">> INSERT emall.shop_order_goods"
    total = dao.select_value( 'select count(*) from ruby.order_items;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_order_goods (id, order_id, goods_id, goods_price, real_price, goods_nums, color)' + \
        'select id, order_id, product_id, unit_price, unit_price, quantity, swap_colors from ruby.order_items'  +' '+\
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
  # 1.limit_price字段 增加字段
  # 2.rule字段 增加长度
  p ">> ALTER emall.shop_coupon"
    alter_sql = 'ALTER TABLE `emall`.`shop_coupon`' +' '+\
      'ADD COLUMN `limit_price` INT(11) NOT NULL DEFAULT 0 AFTER `money`,' +' '+\
      'CHANGE COLUMN `rule` `rule` VARCHAR(255) NOT NULL COMMENT "优惠券活动规则" ;'
    dao.execute(alter_sql)
  print_finish

  p ">> INSERT emall.shop_coupon"
    insert_sql = 'insert into emall.shop_coupon (name, rule, money, limit_price, startTime, endTime, updateTime, isActivity)' +' '+\
      'select coupon_string, coupon_name, discount_fee, COALESCE(`limit_price`, 0), max(coupon_start), max(coupon_end), max(coupon_end), 0' +' '+\
      'from ruby.coupons where coupon_string IS NOT null group by coupon_string order by max(coupon_start)'
    dao.execute(insert_sql)
  print_finish

  p ">> INSERT emall.shop_coupon_number"
    total = dao.select_value( 'select count(*) from ruby.coupons;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_coupon_number (id, number, cid, userid, money, status, isUse, startTime, endTime, createTime)' + \
        'select id, coupon_number, 0, user_id, discount_fee, 1, COALESCE(`is_used`, 1), coupon_start, coupon_end, created_at' +' '+\
        "from ruby.coupons where id > #{start_id} and id <= #{1000 + start_id} " +' '+\
        'AND coupon_string IS NOT null AND user_id IS NOT null'
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
    # todo TANSFER cid
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

private
def print_time
  Time.now.strftime("%F %T")
end

def print_finish
  p '>> FINISHED at ' + print_time
end

def dao
  ActiveRecord::Base.connection
end

self.perform