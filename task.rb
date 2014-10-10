require 'active_record'
require 'mysql2'
require 'yaml'

config = YAML::load(File.open(File.dirname(__FILE__) + "/database.yml"))
ActiveRecord::Base.establish_connection(config)

def self.perform
  p "=== task starts at #{print_time} ==="
  import_shop_user
  import_shop_member
  import_shop_order
  p "=== task ends at #{print_time} ==="
end

def import_shop_user
  p ">> DELETE emall.shop_user"
  dao.execute( 'delete from emall.shop_user;' )
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
  dao.execute( 'delete from emall.shop_member;' )
  print_finish

  # emall.shop_member 要alter字段
    # 1.qq字段 增加长度
    # 2.sex字段 修改备注
  p ">> ALTER emall.shop_member"
    alter_sql = 'ALTER TABLE `emall`.`shop_member`' +' '+\
      'CHANGE COLUMN `qq` `qq` VARCHAR(255) NULL DEFAULT NULL COMMENT "QQ",' +' '+\
      'CHANGE COLUMN `sex` `sex` TINYINT(1) NOT NULL DEFAULT "1" COMMENT "性别1男2女3保密" ;'
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
  ids = dao.select_values( 'select id from ruby.users where blocked_at is not null;' )
  dao.execute( "update emall.shop_member set status = 3 where user_id in (#{ ids.join(',') });" )
  print_finish
end

def import_shop_order
  p ">> DELETE emall.shop_order"
  dao.execute( 'delete from emall.shop_order;' )
  print_finish

  # emall.shop_order 要alter字段
    # 1.order_type字段 增加长度
  p ">> ALTER emall.shop_order"
    alter_sql = 'ALTER TABLE `emall`.`shop_order`' +' '+\
      'CHANGE COLUMN `order_type` `order_type` VARCHAR(255) NOT NULL COMMENT "订单类型";'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_order 44列
    # INSERT 32列
    # 废弃 pay_status, distribution_status, if_del, insured, if_insured, pay_fee, taxes, discount, if_print, prop, exp, type
  p ">> INSERT emall.shop_order"
    total = dao.select_value( 'select count(*) from ruby.orders;' )
    p "   total #{total} records"
    start_id = 0
    while start_id < total
      insert_sql = 'insert into emall.shop_order' +' '+\
        '(id, order_no, user_id, pay_type, status, accept_name, postcode, telphone, address, mobile,' +' '+\
        '  payable_amount, real_amount, payable_freight, real_freight, pay_time, send_time, create_time, completion_time,' +' '+\
        '  invoice, postscript, note, invoice_title, order_amount, accept_time, point, order_type)' +' '+\
        'select id, \'_old_\', user_id, 5, state, shipping_contact_name, shipping_zipcode, shipping_telephone, shipping_address, shipping_mobile,' +' '+\
        '  items_total_price, items_total_price, shipping_fee, shipping_fee, deal_time, send_goods_at, created_at, complete_time,' +' '+\
        '  0, buyer_order_message, seller_memo, invoice_title, total_price, complete_time, integral, order_type' +' '+\
        "from ruby.orders where id > #{start_id} and id <= #{1000 + start_id} AND items_total_price IS NOT null;"
      dao.execute(insert_sql)
      start_id = start_id + 1000
    end
    # todo TANSFER order_no => Order::number
    # todo TANSFER distribution 不用关联 shop_delivery 直接保存立邦ERP的发货信息
    # todo TANSFER country
    # todo TANSFER province
    # todo TANSFER city
    # todo TANSFER area
    # todo TANSFER invoice
    # todo TANSFER promotions => Order::discount_fee
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