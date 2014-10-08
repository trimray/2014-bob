require 'active_record'
require 'mysql2'
require 'yaml'

config = YAML::load(File.open(File.dirname(__FILE__) + "/database.yml"))
ActiveRecord::Base.establish_connection(config)

def self.perform
  p "=== task start #{print_time} ==="
end

private
def print_time
  Time.now.strftime("%F %T")
end

self.perform