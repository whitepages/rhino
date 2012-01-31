require 'java'

java_import org.apache.hadoop.hbase.client.Scan
java_import org.apache.hadoop.hbase.client.ResultScanner
java_import org.apache.hadoop.hbase.filter.PrefixFilter

module Rhino
  module HBaseNativeInterface
    class Scanner
      include Enumerable

      attr_reader :htable

      def initialize(htable, opts={})
        @htable = htable
        @opts = opts

        open_scanner
      end

      def open_scanner
        @scan_criteria = if @opts[:start_row] && @opts[:stop_row]
          org.apache.hadoop.hbase.client.Scan.new(@opts[:start_row].to_java_bytes, @opts[:stop_row].to_java_bytes)
        elsif @opts[:start_row]
          org.apache.hadoop.hbase.client.Scan.new(@opts[:start_row].to_java_bytes)
        else
          org.apache.hadoop.hbase.client.Scan.new()
        end

        if @opts[:columns]
          @opts[:columns].each do |col|
            col_split = col.split(':')
            family = col_split[0]
            qualifier = col_split[1]

            @scan_criteria.addColumn(family.to_java_bytes, qualifier.to_java_bytes)
          end
        end

        @scan_criteria.setFilter(org.apache.hadoop.hbase.filter.PrefixFilter.new(@opts[:starts_with_prefix].to_java_bytes)) if @opts[:starts_with_prefix]

        @scanner = @htable.getScanner(@scan_criteria)
      end

      def close_scanner
        @scanner.close()
        return nil
      end

      def next_row
        begin
          rowresult = @scanner.next()
          return self.close_scanner() if rowresult.nil?

          row = @htable.prepare_rowresult(rowresult)
          return row
        rescue Java::IOException
          return self.close_scanner()
        end
      end

      def each
        while row = self.next_row()
          yield(row)
        end
      end
    end
  end
end
