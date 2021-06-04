require 'db'
require 'db/mariadb'
require "active_record/connection_adapters/abstract_mysql_adapter"
require "active_record/connection_adapters/mysql/database_statements"

# TODO: move to the gem?
module DB
	module MariaDB
		class Connection < Async::Pool::Resource
      def affected_rows
        @native.affected_rows
      end
    end
  end
end

module ActiveRecord
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects.
    def async_mariadb_connection(config)
      
    end
  end

  module ConnectionAdapters
    class AsyncMariaDBConnection < AbstractMysqlAdapter
      include ActiveRecord::ConnectionAdapters::MySQL::DatabaseStatements

      def initialize(connection, logger, connection_options, config)
        superclass_config = config.reverse_merge(prepared_statements: false)
        super(connection, logger, connection_options, superclass_config)
      end

      def execute(query, name = nil, async: false)
        @connection.send_query(query)
        @connection.next_result
      end

      def exec_query(sql, name = "SQL", binds = [], prepare: false, async: false)
        result = execute(sql)
        return unless result
        ActiveRecord::Result.new(result.fields.map(&:name), result.to_a)
      end

      def last_inserted_id(_result)
        @connection.instance_variable_get(:@native).insert_id
      end

      def abandon_results!
        @connection.instance_variable_get(:@native).discard_results
      end

      def concurrency
        # dunno what this should be but it was supposed to respond to this method
        1
      end

      def close
        @connection.close
      end

      def full_version
        schema_cache.database_version.full_version_string
      end

      def get_full_version
        # @connection.server_info[:version]
        # DB::MariaDB::Native has `info` method but it returns nil
        execute("select version()").to_a.first.first
      end
      
      def each_hash(result) # :nodoc:
        if block_given?
          f = result.fields
          result.each do |row|
            # TODO: can DB::MariaDB::Native provide result as a hash?
            h = {}
            f.each_with_index do |r, i|
              h[r.name.to_sym] = row[i]
            end
            yield h
          end
        else
          to_enum(:each_hash, result)
        end
      end
    end
  end
end

class AsyncMariaDBPool
  # include MonitorMixin
  # include QueryCache::ConnectionPoolConfiguration
  # include ConnectionAdapters::AbstractPool

  attr_accessor :automatic_reconnect, :checkout_timeout
  attr_reader :db_config, :size, :pool_config

  delegate :schema_cache, :schema_cache=, to: :pool_config

  def initialize(pool_config)
    @pool_config = pool_config
    @db_config = pool_config.db_config

    adapter = DB::MariaDB::Adapter.new(
      @db_config.configuration_hash.slice(:host, :username, :database)
    )
    @pool = Async::Pool::Controller.new((-> { 
      ActiveRecord::ConnectionAdapters::AsyncMariaDBConnection.new(
        adapter.call,
        ActiveRecord::Base.logger,
        nil,
        {}
      )
    }))

    # @pool.acquire
  end

  # This is supposed to maintain fiber/thread-local connection
  # Right now, it would always give you a new connection
  def connection
    checkout
  end

  # Returns true if there is an open connection being used for the current thread.
  #
  # This method only works for connections that have been obtained through
  # #connection or #with_connection methods. Connections obtained through
  # #checkout will not be detected by #active_connection?
  def active_connection?
    # TODO
    false
  end

  # Signal that the thread is finished with the current connection.
  # #release_connection releases the connection-thread association
  # and returns the connection to the pool.
  #
  # This method only works for connections that have been obtained through
  # #connection or #with_connection methods, connections obtained through
  # #checkout will not be automatically released.
  def release_connection(owner_thread = Thread.current)
    if conn = @thread_cached_conns.delete(connection_cache_key(owner_thread))
      checkin conn
    end
  end

  # If a connection obtained through #connection or #with_connection methods
  # already exists yield it to the block. If no such connection
  # exists checkout a connection, yield it to the block, and checkin the
  # connection when finished.
  def with_connection
    unless conn = @thread_cached_conns[connection_cache_key(Thread.current)]
      conn = connection
      fresh_connection = true
    end
    yield conn
  ensure
    release_connection if fresh_connection
  end

  # Returns true if a connection has already been opened.
  # def connected?
  #   synchronize { @connections.any? }
  # end

  # Returns an array containing the connections currently in the pool.
  # Access to the array does not require synchronization on the pool because
  # the array is newly created and not retained by the pool.
  #
  # However; this method bypasses the ConnectionPool's thread-safe connection
  # access pattern. A returned connection may be owned by another thread,
  # unowned, or by happen-stance owned by the calling thread.
  #
  # Calling methods on a connection without ownership is subject to the
  # thread-safety guarantees of the underlying method. Many of the methods
  # on connection adapter classes are inherently multi-thread unsafe.
  # def connections
  #   synchronize { @connections.dup }
  # end

  # Disconnects all connections in the pool, and clears the pool.
  #
  # Raises:
  # - ActiveRecord::ExclusiveConnectionTimeoutError if unable to gain ownership of all
  #   connections in the pool within a timeout interval (default duration is
  #   <tt>spec.db_config.checkout_timeout * 2</tt> seconds).
  # def disconnect(raise_on_acquisition_timeout = true)
  #   with_exclusively_acquired_all_connections(raise_on_acquisition_timeout) do
  #     synchronize do
  #       @connections.each do |conn|
  #         if conn.in_use?
  #           conn.steal!
  #           checkin conn
  #         end
  #         conn.disconnect!
  #       end
  #       @connections = []
  #       @available.clear
  #     end
  #   end
  # end

  # Disconnects all connections in the pool, and clears the pool.
  #
  # The pool first tries to gain ownership of all connections. If unable to
  # do so within a timeout interval (default duration is
  # <tt>spec.db_config.checkout_timeout * 2</tt> seconds), then the pool is forcefully
  # disconnected without any regard for other connection owning threads.
  # def disconnect!
  #   disconnect(false)
  # end

  # Discards all connections in the pool (even if they're currently
  # leased!), along with the pool itself. Any further interaction with the
  # pool (except #spec and #schema_cache) is undefined.
  #
  # See AbstractAdapter#discard!
  # def discard! # :nodoc:
  #   synchronize do
  #     return if self.discarded?
  #     @connections.each do |conn|
  #       conn.discard!
  #     end
  #     @connections = @available = @thread_cached_conns = nil
  #   end
  # end

  # def discarded? # :nodoc:
  #   @connections.nil?
  # end

  # Clears the cache which maps classes and re-connects connections that
  # require reloading.
  #
  # Raises:
  # - ActiveRecord::ExclusiveConnectionTimeoutError if unable to gain ownership of all
  #   connections in the pool within a timeout interval (default duration is
  #   <tt>spec.db_config.checkout_timeout * 2</tt> seconds).
  # def clear_reloadable_connections(raise_on_acquisition_timeout = true)
  #   with_exclusively_acquired_all_connections(raise_on_acquisition_timeout) do
  #     synchronize do
  #       @connections.each do |conn|
  #         if conn.in_use?
  #           conn.steal!
  #           checkin conn
  #         end
  #         conn.disconnect! if conn.requires_reloading?
  #       end
  #       @connections.delete_if(&:requires_reloading?)
  #       @available.clear
  #     end
  #   end
  # end

  # Clears the cache which maps classes and re-connects connections that
  # require reloading.
  #
  # The pool first tries to gain ownership of all connections. If unable to
  # do so within a timeout interval (default duration is
  # <tt>spec.db_config.checkout_timeout * 2</tt> seconds), then the pool forcefully
  # clears the cache and reloads connections without any regard for other
  # connection owning threads.
  # def clear_reloadable_connections!
  #   clear_reloadable_connections(false)
  # end

  # Check-out a database connection from the pool, indicating that you want
  # to use it. You should call #checkin when you no longer need this.
  #
  # This is done by either returning and leasing existing connection, or by
  # creating a new connection and leasing it.
  #
  # If all connections are leased and the pool is at capacity (meaning the
  # number of currently leased connections is greater than or equal to the
  # size limit set), an ActiveRecord::ConnectionTimeoutError exception will be raised.
  #
  # Returns: an AbstractAdapter object.
  #
  # Raises:
  # - ActiveRecord::ConnectionTimeoutError no connection can be obtained from the pool.
  def checkout(checkout_timeout = @checkout_timeout)
    @pool.acquire
    # checkout_and_verify(acquire_connection(checkout_timeout))
  end

  # Check-in a database connection back into the pool, indicating that you
  # no longer need this connection.
  #
  # +conn+: an AbstractAdapter object, which was obtained by earlier by
  # calling #checkout on this pool.
  # def checkin(conn)
  #   conn.lock.synchronize do
  #     synchronize do
  #       remove_connection_from_thread_cache conn

  #       conn._run_checkin_callbacks do
  #         conn.expire
  #       end

  #       @available.add conn
  #     end
  #   end
  # end

  # Remove a connection from the connection pool. The connection will
  # remain open and active but will no longer be managed by this pool.
  # def remove(conn)
  #   needs_new_connection = false

  #   synchronize do
  #     remove_connection_from_thread_cache conn

  #     @connections.delete conn
  #     @available.delete conn

  #     # @available.any_waiting? => true means that prior to removing this
  #     # conn, the pool was at its max size (@connections.size == @size).
  #     # This would mean that any threads stuck waiting in the queue wouldn't
  #     # know they could checkout_new_connection, so let's do it for them.
  #     # Because condition-wait loop is encapsulated in the Queue class
  #     # (that in turn is oblivious to ConnectionPool implementation), threads
  #     # that are "stuck" there are helpless. They have no way of creating
  #     # new connections and are completely reliant on us feeding available
  #     # connections into the Queue.
  #     needs_new_connection = @available.any_waiting?
  #   end

  #   # This is intentionally done outside of the synchronized section as we
  #   # would like not to hold the main mutex while checking out new connections.
  #   # Thus there is some chance that needs_new_connection information is now
  #   # stale, we can live with that (bulk_make_new_connections will make
  #   # sure not to exceed the pool's @size limit).
  #   bulk_make_new_connections(1) if needs_new_connection
  # end

  # # Recover lost connections for the pool. A lost connection can occur if
  # # a programmer forgets to checkin a connection at the end of a thread
  # # or a thread dies unexpectedly.
  # def reap
  #   stale_connections = synchronize do
  #     return if self.discarded?
  #     @connections.select do |conn|
  #       conn.in_use? && !conn.owner.alive?
  #     end.each do |conn|
  #       conn.steal!
  #     end
  #   end

  #   stale_connections.each do |conn|
  #     if conn.active?
  #       conn.reset!
  #       checkin conn
  #     else
  #       remove conn
  #     end
  #   end
  # end

  # # Disconnect all connections that have been idle for at least
  # # +minimum_idle+ seconds. Connections currently checked out, or that were
  # # checked in less than +minimum_idle+ seconds ago, are unaffected.
  # def flush(minimum_idle = @idle_timeout)
  #   return if minimum_idle.nil?

  #   idle_connections = synchronize do
  #     return if self.discarded?
  #     @connections.select do |conn|
  #       !conn.in_use? && conn.seconds_idle >= minimum_idle
  #     end.each do |conn|
  #       conn.lease

  #       @available.delete conn
  #       @connections.delete conn
  #     end
  #   end

  #   idle_connections.each do |conn|
  #     conn.disconnect!
  #   end
  # end

  # # Disconnect all currently idle connections. Connections currently checked
  # # out are unaffected.
  # def flush!
  #   reap
  #   flush(-1)
  # end

  # def num_waiting_in_queue # :nodoc:
  #   @available.num_waiting
  # end

  # Return connection pool's usage statistic
  # Example:
  #
  #    ActiveRecord::Base.connection_pool.stat # => { size: 15, connections: 1, busy: 1, dead: 0, idle: 0, waiting: 0, checkout_timeout: 5 }
  def stat
    synchronize do
      {
        size: size,
        connections: @connections.size,
        busy: @connections.count { |c| c.in_use? && c.owner.alive? },
        dead: @connections.count { |c| c.in_use? && !c.owner.alive? },
        idle: @connections.count { |c| !c.in_use? },
        waiting: num_waiting_in_queue,
        checkout_timeout: checkout_timeout
      }
    end
  end
end
