require 'java'
require 'rhino/interface/hbase-native/scanner'

java_import java.lang.System
java_import org.apache.hadoop.hbase.HTableDescriptor
java_import org.apache.hadoop.hbase.HColumnDescriptor
java_import org.apache.hadoop.hbase.client.HTablePool
java_import org.apache.hadoop.hbase.client.HTableInterface
java_import org.apache.hadoop.hbase.KeyValue
java_import org.apache.hadoop.hbase.client.Get
java_import org.apache.hadoop.hbase.client.Put
java_import org.apache.hadoop.hbase.client.Delete

module Rhino
  module HBaseNativeInterface
    class Table < Rhino::Interface::Table
      DEFAULT_POOL_SIZE = 10

      attr_reader :hbase, :table_name

      def initialize(hbase, table_name, opts = {})

        raise LoadError, "Unsupported platform, Rhino::HBaseNativeInterface::Table requires the JRuby platform" unless RUBY_PLATFORM == "java"

        @hbase = hbase
        @table_name = table_name
        @opts = opts
        pool_size = opts.fetch(:table_pool_size, DEFAULT_POOL_SIZE)
        @table_pool = org.apache.hadoop.hbase.client.HTablePool.new(@hbase.client.getConfiguration(), pool_size)
      end

      def create_table(column_families)
        table = org.apache.hadoop.hbase.HTableDescriptor.new(table_name.to_s)

        column_families.each do |family_name|
          descriptor = org.apache.hadoop.hbase.HColumnDescriptor.new(family_name.to_s)
          table.addFamily(descriptor)
        end

        @hbase.createTable(table)
      end

      def exists?
        @hbase.tableExists(table_name)
      end

      def delete_table
        @hbase.disableTable(table_name)
        @hbase.deleteTable(table_name)
      end

      def get(*rowkeys)
        opts = rowkeys.extract_options!

        debug("#{self.class.name}#get(#{rowkeys.inspect}, #{opts.inspect})")

        raise(ArgumentError, "get requires a key") if rowkeys.nil? or rowkeys.empty? or rowkeys[0]==''

        # TODO: add java filter support

        column_families_to_get = Array(opts.delete(:column_families)).compact
        timestamp = opts.delete(:timestamp)
        timestamp = timestamp.to_i if timestamp

        gets = rowkeys.collect do |key|
          get_descriptor = org.apache.hadoop.hbase.client.Get.new(key.to_java_bytes)
          get_descriptor.setTimeRange(0, timestamp + 1) if timestamp
          column_families_to_get.each { |column_name| get_descriptor.addFamily(column_name.to_java_bytes) }
          get_descriptor
        end

        results = execute_with_table_from_pool do |table_iface|
          table_iface.get(gets)
        end

        raise Rhino::Interface::Table::RowNotFound, "Request for keys #{rowkeys.inspect} returned no results" if results.nil?

        prepped_results = results.map { |r| prepare_rowresult(r) }

        return prepped_results
      end

      def put(rowkey, data, timestamp = nil)
        timestamp = timestamp.to_i if timestamp

        # separate out data insert/update mutations versus column delete mutations
        puts = nil
        deletes = nil

        data.each do |key, val|
          family, qualifier = key.split(':', 2)

          if (val)
            args = [ rowkey.to_java_bytes ]
            args << timestamp if timestamp
            puts = org.apache.hadoop.hbase.client.Put.new( * args ) if puts.nil?

            puts.add(family.to_java_bytes, qualifier.to_java_bytes, val.to_java_bytes)
          else
            deletes = org.apache.hadoop.hbase.client.Delete.new(rowkey.to_java_bytes) if deletes.nil?
            deletes.deleteColumns(family.to_java_bytes, qualifier.to_java_bytes)
          end
        end

        execute_with_table_from_pool do |table_iface|
          table_iface.put(puts) if puts
          table_iface.delete(deletes) if deletes
        end
      end

      def delete_row(key, opts = {})
        args = [ key.to_java_bytes ]
        args << opts[:timestamp] << nil if opts[:timestamp]
        delete = org.apache.hadoop.hbase.client.Delete.new( * args )

        execute_with_table_from_pool do |table_iface|
          table_iface.delete(delete)
        end
      end

      def delete_all_rows(opts = {})
        scan.each do |row|
          delete_row(row['key'], opts)
        end
      end

      def scan(opts={})
        return Rhino::HBaseNativeInterface::Scanner.new(self, opts)
      end

      def get_table
        @table_pool.getTable(table_name)
      end

      private
      def execute_with_table_from_pool
        response = nil
        table_iface = get_table()
        response = yield(table_iface)
      rescue IOException => e # TODO: actually rescue some meaningful errors here
        raise Rhino::Interface::Table::TableNotFound, e.message
      ensure
        table_iface.close()

        return response
      end

      public
      def prepare_rowresult(row)
        rowkey = row.getRow()
        return {} if rowkey.nil?

        columns = row.list()
        data = {}

        data['timestamp'] = -1
        data['key'] = String.from_java_bytes(rowkey) # this corresponds to the rowkey

        columns.each do |kvp|
          family = String.from_java_bytes(kvp.getFamily())
          qualifier = String.from_java_bytes(kvp.getQualifier())
          value = String.from_java_bytes(kvp.getValue())
          timestamp = kvp.getTimestamp()

          data['timestamp'] = timestamp if data['timestamp'] < timestamp # set the row timestamp to be the largest timestamp for all cells

          # Build a TCell so we can collect the per-cell timestamp in the Rhino::Cell
          tcell = Apache::Hadoop::Hbase::Thrift::TCell.new
          tcell.timestamp = timestamp
          tcell.value = value

          data["#{family}:#{qualifier}"] = tcell
        end

        return data
      end
    end
  end
end
