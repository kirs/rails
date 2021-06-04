# frozen_string_literal: true

require "active_record"
require 'byebug'
require 'mysql2'

class Person < ActiveRecord::Base
  # establish_connection adapter: "sqlite3", database: ":memory:"
  # connection.create_table table_name, force: true do |t|
  #   t.string :name
  # end
end

def demo
  Person.create(name: "kirs")

  pers = Person.first
  pp pers
  pers.destroy
end

if ENV['NATIVE']
  Person.establish_connection(
    adapter: 'mysql2', host: '127.0.0.1', username: 'root', database: 'test'
  )
  demo
  exit 1
end

require 'async'
Sync do
  Person.establish_connection(
    adapter: 'async_mariadb',
    host: '127.0.0.1',
    username: 'root',
    database: 'test',
    pool_class: 'AsyncMariaDBPool'
  )

  Person.connection.create_table Person.table_name, force: true do |t|
    t.string :name
  end
  conn = Person.connection
  puts conn.execute("SELECT VERSION()").inspect

  demo
end
