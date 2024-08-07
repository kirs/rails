# frozen_string_literal: true

require "cases/helper"
require "support/connection_helper"

module ActiveRecord
  class TransactionTest < ActiveRecord::AbstractMysqlTestCase
    self.use_transactional_tests = false

    class Sample < ActiveRecord::Base
      self.table_name = "samples"
    end

    setup do
      @abort, Thread.abort_on_exception = Thread.abort_on_exception, false
      Thread.report_on_exception, @original_report_on_exception = false, Thread.report_on_exception

      connection = ActiveRecord::Base.lease_connection
      connection.clear_cache!

      connection.transaction do
        connection.drop_table "samples", if_exists: true
        connection.create_table("samples") do |t|
          t.integer "value"
        end
      end

      Sample.reset_column_information
    end

    teardown do
      ActiveRecord::Base.lease_connection.drop_table "samples", if_exists: true

      Thread.abort_on_exception = @abort
      Thread.report_on_exception = @original_report_on_exception
    end

    test "raises Deadlocked when a deadlock is encountered" do
      connection = Sample.lease_connection
      assert_raises(ActiveRecord::Deadlocked) do
        barrier = Concurrent::CyclicBarrier.new(2)

        s1 = Sample.create value: 1
        s2 = Sample.create value: 2

        thread = Thread.new do
          Sample.transaction do
            s1.lock!
            barrier.wait
            s2.update value: 1
          end
        end

        begin
          Sample.transaction do
            s2.lock!
            barrier.wait
            s1.update value: 2
          end
        ensure
          thread.join
        end
      end
      assert_predicate connection, :active?
    end

    test "raises LockWaitTimeout when lock wait timeout exceeded" do
      assert_raises(ActiveRecord::LockWaitTimeout) do
        s = Sample.create!(value: 1)
        latch1 = Concurrent::CountDownLatch.new
        latch2 = Concurrent::CountDownLatch.new

        thread = Thread.new do
          Sample.transaction do
            Sample.lock.find(s.id)
            latch1.count_down
            latch2.wait
          end
        end

        begin
          Sample.transaction do
            latch1.wait
            Sample.lease_connection.execute("SET innodb_lock_wait_timeout = 1")
            Sample.lock.find(s.id)
          end
        ensure
          Sample.lease_connection.execute("SET innodb_lock_wait_timeout = DEFAULT")
          latch2.count_down
          thread.join
        end
      end
    end

    test "raises StatementTimeout when statement timeout exceeded" do
      skip unless ActiveRecord::Base.lease_connection.show_variable("max_execution_time")
      error = assert_raises(ActiveRecord::StatementTimeout) do
        s = Sample.create!(value: 1)
        latch1 = Concurrent::CountDownLatch.new
        latch2 = Concurrent::CountDownLatch.new

        thread = Thread.new do
          Sample.transaction do
            Sample.lock.find(s.id)
            latch1.count_down
            latch2.wait
          end
        end

        begin
          Sample.transaction do
            latch1.wait
            Sample.lease_connection.execute("SET max_execution_time = 1")
            Sample.lock.find(s.id)
          end
        ensure
          Sample.lease_connection.execute("SET max_execution_time = DEFAULT")
          latch2.count_down
          thread.join
        end
      end
      assert_kind_of ActiveRecord::QueryAborted, error
    end

    test "raises QueryCanceled when canceling statement due to user request" do
      error = assert_raises(ActiveRecord::QueryCanceled) do
        s = Sample.create!(value: 1)
        latch = Concurrent::CountDownLatch.new

        thread = Thread.new do
          Sample.transaction do
            Sample.lock.find(s.id)
            latch.count_down
            sleep(0.5)
            conn = Sample.lease_connection
            pid = conn.query_value("SELECT id FROM information_schema.processlist WHERE info LIKE '% FOR UPDATE'")
            conn.execute("KILL QUERY #{pid}")
          end
        end

        begin
          Sample.transaction do
            latch.wait
            Sample.lock.find(s.id)
          end
        ensure
          thread.join
        end
      end
      assert_kind_of ActiveRecord::QueryAborted, error
    end

    test "reconnect preserves isolation level" do
      ActiveRecord::Base.logger = Logger.new(STDOUT).tap do |l|
        l.level = Logger::DEBUG
      end

      @connection = Sample.lease_connection
      @connection.instance_eval do
        # Simulates the first BEGIN attempt failing
        def perform_query(raw_connection, sql, binds, type_casted_binds, **kwargs)
          if sql == "BEGIN" && !@first_begin_failed
            @first_begin_failed = true
            raise ActiveRecord::DatabaseConnectionError
          end
          super
        end
      end

      Sample.transaction(isolation: :read_committed) do
        @connection.materialize_transactions
        isolation = @connection.select_value("SELECT ISOLATION_LEVEL FROM performance_schema.events_transactions_current where THREAD_ID=PS_CURRENT_THREAD_ID()")
        assert_equal "READ COMMITTED", isolation
      end
    end
  end
end
