require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/mysql_adapter'
require 'action_controller'
require 'action_controller/base'
require 'sync'
require 'digest/sha1'

module ActiveRecord
  class Base
    # Establishes a connection to the database that's used by all Active Record objects.
    def self.mysql_cluster_connection(config)
      # Require the MySQL driver and define Mysql::Result.all_hashes
      unless defined? Mysql
        begin
          require_library_or_gem('mysql')
        rescue LoadError
          $stderr.puts '!!! The bundled mysql.rb driver has been removed from Rails 2.2. Please install the mysql gem and try again: gem install mysql.'
          raise
        end
      end

      ConnectionAdapters::MysqlClusterAdapter.new(logger, config)
    end
  end

  module ConnectionAdapters
    class MysqlClusterAdapter
      @@logger = nil
      @@current = nil
      cattr_reader :logger

      @pool = nil

      def initialize(logger, config)
        logger.info 'MysqlClusterAdapter initialize'

        @@logger = logger
        config = config.symbolize_keys
        @pool = Pool.new(config[:retry])

        config[:nodes].each do |node|
          @pool.add_node node
        end
      end

      def adapter_name
        'MySQLCluster'
      end

      def method_missing(method, *arguments, &block)
        @@current = @pool.active_node unless @@current
        @@current.send(method, *arguments, &block)
      end


      # class method

      class << self
        def initialize_request
          @@current = nil
        end

        def method_missing(method, *arguments, &block)
          ConnectionAdapters::MysqlAdapter.send(method, *arguments, &block)
        end
      end


      # MysqlAdapter pool

      class Pool
        def initialize(retry_default)
          @retry_default = retry_default ? retry_default.to_i : 60
          @nodes = []
        end

        def add_node(config)
          config = config.symbolize_keys
          config[:retry] = config[:retry] ? config[:retry].to_i : @retry_default
          @nodes << Node.new(config)
        end

        def active_node
          #MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool active_node caller: #{caller(0)}"
          len = @nodes.length
          n = rand(len)

          # search active node
          i = 0
          while i<len
            node = @nodes[(n+i) % len]
            if node.connected? && node.active?
              MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool active_node: #{node.config[:host]}:#{node.config[:port]}"
              return node
            else
              # async connect
              node.reconnect! rescue false
            end
            i += 1
          end

          # join thread
          MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool: join thread"
          @nodes.each {|node|
            if node.t
              node.t.join
              if node.connected?
                MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool active_node: #{node.config[:host]}:#{node.config[:port]}"
                return node
              end
            end
          }

          # sync connect
          MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool: sync connect"
          @nodes.each {|node|
            node.connect!
            node.t.join if node.t
            if node.connected?
              MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool active_node: #{node.config[:host]}:#{node.config[:port]}"
              return node
            end
          }

          raise 'MysqlClusterAdapter::Pool: Nodes ZENMETSU!!'
        end
      end


      # MysqlAdapter node

      class Node
        @connection = nil
        @config = nil
        @next_retry = 0
        @connected = false
        @t = nil
        @sync = nil
        attr_reader :config, :t

        def initialize(config)
          @config = config
          @sync = Sync.new
          connect!
        end

        def connect!
          host     = @config[:host]
          port     = @config[:port]
          socket   = @config[:socket]
          username = @config[:username] ? @config[:username].to_s : 'root'
          password = @config[:password].to_s
          database = @config[:database]

          MysqlCompat.define_all_hashes_method!

          mysql = Mysql.init
          mysql.ssl_set(@config[:sslkey], @config[:sslcert], @config[:sslca], @config[:sslcapath], @config[:sslcipher]) if @config[:sslca] || @config[:sslkey]

          default_flags = Mysql.const_defined?(:CLIENT_MULTI_RESULTS) ? Mysql::CLIENT_MULTI_RESULTS : 0
          options = [host, username, password, database, port, socket, default_flags]

          begin
            @connection = ConnectionAdapters::MysqlAdapter.new(mysql, MysqlClusterAdapter.logger, options, @config)
            @connected = true
            MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Node connection ok: #{@config[:host]}:#{@config[:port]}"
          rescue
            @connection = nil
            @connected = false
            @next_retry = Time.now.to_i + @config[:retry].to_i
            MysqlClusterAdapter.logger.warn "MysqlClusterAdapter::Node connection error: #{@config[:host]}:#{@config[:port]}"
          end
        end

        def reconnect!
          if connected?
            begin
              @connection.reconnect!
            rescue => e
              exec_flag = false
              @sync.synchronize {
                if connected?
                  @connected = false
                  @next_retry = Time.now.to_i + 1000
                  exec_flag = true
                end
              }
              if exec_flag
                MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Node reconnecting: #{@config[:host]}:#{@config[:port]}"
                connect! rescue @next_retry = Time.now.to_i + @config[:retry].to_i
              end
              raise e
            end
          else
            exec_flag = false
            @sync.synchronize {
              if @next_retry.to_i <= Time.now.to_i && (@t==nil || !@t.alive?)
                @next_retry = Time.now.to_i + 1000
                exec_flag = true
              end
            }
            if exec_flag
              @t = Thread.new do
                MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Node reconnecting: #{@config[:host]}:#{@config[:port]}"
                connect! rescue @next_retry = Time.now.to_i + @config[:retry].to_i
              end
            end
          end
        end

        def connected?
          @connected
        end

        def active?
          @connection.active? rescue false
        end

        def method_missing(method, *arguments, &block)
          begin
            @connection.send(method, *arguments, &block)

          rescue NoMethodError, Mysql::Error, ActiveRecord::StatementInvalid => e
            # NoMethodError
            @connected = false if @connected && e.to_s.index('for nil:NilClass')
            @connected = false if @connected && e.to_s.index("undefined method `collect!'")

            # Mysql::Error
            @connected = false if @connected && e.to_s.index("Can't connect to MySQL server")

            # ActiveRecord::StatementInvalid
            @connected = false if @connected && e.to_s.index('MySQL server has gone away')
            @connected = false if @connected && e.to_s.index("Lost connection to MySQL server during query")
            @connected = false if @connected && e.to_s.index(" from NDBCLUSTER: ")

            # active?
            @connected = false if @connected && (!@connection.active? rescue false)

            if connected?
              raise e
            else
              MysqlClusterAdapter.logger.warn "MysqlClusterAdapter::Node disconnected: #{@config[:host]}:#{@config[:port]}"
              @next_retry = Time.now.to_i + @config[:retry].to_i
            end
          end
        end
      end
    end
  end
end



# for passenger
if defined? PhusionPassenger::Rack::RequestHandler
  module PhusionPassenger
    module Rack
      class RequestHandler
        alias process_request_org_mysql_cluster_adapter process_request

        def process_request(env, input, output)
          ActiveRecord::ConnectionAdapters::MysqlClusterAdapter.initialize_request
          send :process_request_org_mysql_cluster_adapter, env, input, output
        end
      end
    end
  end

# for ./script/server
else
  module ActionController
    class Base
      alias process_org_mysql_cluster_adapter process

      def process(request, response, method = :perform_action, *arguments)
        ActiveRecord::ConnectionAdapters::MysqlClusterAdapter.initialize_request
        send :process_org_mysql_cluster_adapter, request, response, method, *arguments
      end
    end
  end
end
