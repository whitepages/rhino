require 'java'

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
      attr_reader :hbase, :table_name

      def initialize(hbase, table_name, opts = {})
        @hbase = hbase
        @table_name = table_name
        @opts = opts
        pool_size = opts[:table_pool_size] || 10
        @table_pool = org.apache.hadoop.hbase.client.HTablePool.new(@hbase.client.getConfiguration(), pool_size)
      end

      def create_table(column_families)
        table = org.apache.hadoop.hbase.HTableDescriptor.new(self.table_name.to_s)

        column_families.each do |family_name|
          descriptor = org.apache.hadoop.hbase.HColumnDescriptor.new(family_name.to_s)
          table.addFamily(descriptor)
        end

        @hbase.createTable(table)
      end

      def exists?
        @hbase.tableExists(self.table_name)
      end

      def delete_table
        @hbase.disableTable(self.table_name)
        @hbase.deleteTable(self.table_name)
      end

      def get(*rowkeys)
        opts = rowkeys.extract_options!

        debug("#{self.class.name}#get(#{rowkeys.inspect}, #{opts.inspect})")

        raise(ArgumentError, "get requires a key") if rowkeys.nil? or rowkeys.empty? or rowkeys[0]==''

        # TODO: add java filter support

        column_families_to_get = Array(opts.delete(:column_families)).compact
        timestamp = opts.delete(:timestamp)
        timestamp = timestamp.to_i if timestamp
        get_descriptor.setTimeStamp(timestamp) if timestamp

        gets = rowkeys.collect do |key|
          get_descriptor = org.apache.hadoop.hbase.client.Get.new(key.to_java_bytes)
          column_families_to_get.each { |column_name| get_descriptor.addFamily(column_name.to_java_bytes) }
          get_descriptor
        end

        prepped_results = nil

        results = execute_with_table_from_pool do |table_iface|
          table_iface.get(gets)
        end

        raise Rhino::Interface::Table::RowNotFound, "Request for keys #{rowkeys.inspect} returned no results" if results.nil?

        prepped_results = []

        (0..results.size()-1).each do |idx|
          prepped_results << prepare_rowresult(results[idx])
        end

        return prepped_results
      end

      def put(rowkey, data, timestamp = nil)
        timestamp = timestamp.to_i if timestamp

        # separate out data insert/update mutations versus column delete mutations
        puts = nil
        deletes = nil

        data.each do |key, val|
          split_keys = key.split(':')
          family = split_keys[0]
          qualifier = split_keys[1]

          unless (val.nil?)
            if (puts.nil?)
              if (timestamp.nil?)
                puts = org.apache.hadoop.hbase.client.Put.new(rowkey.to_java_bytes)
              else
                puts = org.apache.hadoop.hbase.client.Put.new(rowkey.to_java_bytes, timestamp)
              end
            end
            puts.add(family.to_java_bytes, qualifier.to_java_bytes, val.to_java_bytes)
          else
            deletes = org.apache.hadoop.hbase.client.Delete.new(rowkey.to_java_bytes) if deletes.nil?
            deletes.deleteColumns(family.to_java_bytes, qualifier.to_java_bytes)
          end
        end

        execute_with_table_from_pool do |table_iface|
          table_iface.put(puts) unless puts.nil?
          table_iface.delete(deletes) unless deletes.nil?
        end
      end

      def delete_row(key, opts = {})
        if (opts[:timestamp])
          delete = org.apache.hadoop.hbase.client.Delete.new(key.to_java_bytes, opts[:timestamp], nil)
        else
          delete = org.apache.hadoop.hbase.client.Delete.new(key.to_java_bytes)
        end

        execute_with_table_from_pool do |table_iface|
          table_iface.delete(delete)
        end
      end

      def delete_all_rows(*args)
        scan.each do |row|
          delete_row(row['key'], opts)
        end
      end

      def scan
        return Rhino::HBaseNativeInterface::Scanner.new(self, opts)
      end

      private
      def execute_with_table_from_pool(&blk)
        response = nil
        begin
          table_iface = @table_pool.getTable(self.table_name)
          response = blk.call(table_iface)
        rescue IOException => e # TODO: actually rescue some meaningful errors here
          raise Rhino::Interface::Table::TableNotFound, e.message
        ensure
          table_iface.close()
        end

        return response
      end

      private
      def prepare_rowresult(row)
        columns = row.list()
        data = {}

        data['timestamp'] = -1
        data['key'] = String.from_java_bytes(row.getRow()) # this corresponds to the rowkey

        columns.each do |kvp|
          family = String.from_java_bytes(kvp.getFamily())
          qualifier = String.from_java_bytes(kvp.getQualifier())
          value = String.from_java_bytes(kvp.getValue()) # uh... this needs to be fixed...
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
