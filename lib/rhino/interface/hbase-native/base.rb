require 'java'

module Rhino
  module HBaseNativeInterface
    class Base < Rhino::Interface::Base
      attr_reader :host, :port, :client

      def initialize(host, port)
        debug("Rhino::HBaseNativeInterface::Base.new(#{host.inspect}, #{port.inspect})")
        # TODO: check for JVM and throw if not in JVM

        @client = org.apache.hadoop.hbase.client.HConnection.new

        @host = host
        @port = port
        connect()
      end

      def connect

      end

      def disconnect

      end

      def table_names

      end

      def method_missing(method, *args)

      end
    end
  end
end
