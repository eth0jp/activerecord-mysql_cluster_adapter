require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/mysql_adapter'
require 'action_controller'
require 'action_controller/base'

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
      VERSION = '0.0.1'

      @@logger = nil
      @@current = nil
      cattr_reader :logger

      @pool = nil

      def initialize(logger, config)
        logger.info 'MysqlClusterAdapter initialize'

        @@logger = logger
        config = config.symbolize_keys
        @pool = Pool.new

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
        def initialize()
          @nodes = []
        end

        def add_node(config)
          config = config.symbolize_keys
          @nodes << Node.new(config)
        end

        def active_node
          len = @nodes.length
          n = rand(len)

          # search active node
          i = 0
          while i<len
            node = @nodes[(n+i) % len]
            if node.verify!
              MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Pool active_node: #{node.config[:host]}:#{node.config[:port]}"
              return node
            end
            i += 1
          end

          raise Mysql::Error.new("MysqlClusterAdapter::Pool: All nodes are down!!")
        end
      end


      # MysqlAdapter node

      class Node
        @connection = nil
        @config = nil
        attr_reader :config

        def initialize(config)
          @config = config
        end

        def reconnect!
          begin
            if @connection
              @connection.reconnect!
            else
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

              @connection = ConnectionAdapters::MysqlAdapter.new(mysql, MysqlClusterAdapter.logger, options, @config)
            end
            MysqlClusterAdapter.logger.info "MysqlClusterAdapter::Node connection ok: #{@config[:host]}:#{@config[:port]}"
            true

          rescue
            MysqlClusterAdapter.logger.warn "MysqlClusterAdapter::Node connection error: #{@config[:host]}:#{@config[:port]}"
            false
          end
        end

        def active?
          @connection.active? rescue false
        end

        def verify!
          active? ? true : reconnect!
        end

        def method_missing(method, *arguments, &block)
          @connection.send(method, *arguments, &block)
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
