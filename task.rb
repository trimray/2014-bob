require 'active_record'
require 'mysql2'
require 'yaml'

config = YAML::load(File.open(File.dirname(__FILE__) + "/database.yml"))
ActiveRecord::Base.establish_connection(config)

def self.perform
  p "=== task starts at #{print_time} ==="
  import_shop_user
  import_shop_member
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
  p ">> ALTER emall.shop_member"
    alter_sql = 'ALTER TABLE `emall`.`shop_member`' +' '+\
      'CHANGE COLUMN `qq` `qq` VARCHAR(255) NULL DEFAULT NULL COMMENT "QQ";'
    dao.execute(alter_sql)
  print_finish

  # emall.shop_member 21列
    # INSERT 15列
    # 废弃 exp, message_ids, prop, balance, custom
    # area 设置默认值
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