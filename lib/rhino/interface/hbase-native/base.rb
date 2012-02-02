require 'java'

java_import java.lang.System
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin

module Rhino
  module HBaseNativeInterface
    class Base < Rhino::Interface::Base
      attr_reader :host, :port, :client

      def initialize(host, port)
        debug("Rhino::HBaseNativeInterface::Base.new(#{host.inspect}, #{port.inspect})")

        raise LoadError, "Unsupported platform, Rhino::HBaseNativeInterface:Base requires the JRuby platform" unless RUBY_PLATFORM == "java"

        @host = host
        @port = port
        @config = org.apache.hadoop.hbase.HBaseConfiguration.create()
        @config.set('hbase.zookeeper.quorum', host)
        @config.setBoolean('hbase.cluster.distributed', true)

        connect()
      end

      def connect
        @client = org.apache.hadoop.hbase.client.HBaseAdmin.new(@config)
      end

      def disconnect
        @client.close() unless @client.nil?
      ensure
        @client = nil
      end

      def table_names
        names = []
        @client.listTables().each do |descriptor|
          names.push(descriptor.getNameAsString())
        end

        return names
      end

      def method_missing(method, *args)
        debug("#{self.class.name}#method_missing(#{method.inspect}, #{args.inspect})")
        begin
          connect() if not @client
          client.send(method, *args) if @client
        rescue Java::IOException
          @client = nil
          connect()
          client.send(method, *args) if @client
        end
      end
    end
  end
end
