if RUBY_PLATFORM == "java"
  require 'rhino/interface/hbase-thrift/blocking_socket'
end

module Rhino
  module HBaseThriftInterface
    class Base < Rhino::Interface::Base
      THRIFT_RETRY_COUNT = 3
      attr_reader :host, :port, :client

      def initialize(host, port)
        debug{"Rhino::HBaseThriftInterface::Base.new(#{host.inspect}, #{port.inspect})"}

        @host = host
        @port = port
        connect()
      end

      def connect
        count = 1
        while @client == nil and count < THRIFT_RETRY_COUNT
          socket = if RUBY_PLATFORM == "java"
                     warn("Rhino::HBaseThriftInterface is NOT functional for production loads when running via JRuby.  Standard Thrift::Socket objects don't work because of a jruby bug with how they run async sockets, overriding with Thrift::BlockingSocket, which doesn't implement timeouts at all.  The recommended interface is Rhino::HBaseNativeInterface")
                     Thrift::BlockingSocket.new(host, port)
                   else
                     Thrift::Socket.new(host, port)
                   end

          transport = Thrift::BufferedTransport.new(socket)
          protocol = Thrift::BinaryProtocol.new(transport)
          @client = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(protocol)
          begin
            transport.open()
          rescue Thrift::TransportException => e
            @client = nil
            debug{"Could not connect to HBase.  Retrying in 5 seconds..." + count.to_s + " of " + THRIFT_RETRY_COUNT.to_s}
            sleep 5
            count = count + 1
          end
        end
        if count == THRIFT_RETRY_COUNT
          debug{"Failed to connect to HBase after " + THRIFT_RETRY_COUNT.to_s + " tries."}
        end
      end

      def disconnect()
        @client = nil
      end

      def table_names
        client.getTableNames()
      end

      def method_missing(method, *args)
        debug{"#{self.class.name}#method_missing(#{method.inspect}, #{args.inspect})"}
        begin
          connect() if not @client
          client.send(method, *args) if @client
        rescue Thrift::TransportException
          @client = nil
          connect()
          client.send(method, *args) if @client
        end
      end
    end
  end
end
