require 'java'

java_import org.apache.hadoop.hbase.client.Scan
java_import org.apache.hadoop.hbase.client.ResultScanner
java_import org.apache.hadoop.hbase.filter.PrefixFilter

module Rhino
  module HBaseNativeJavaInterface
    class Scanner
      include Enumerable

      attr_reader :htable

      def initialize(htable, opts={})

        raise LoadError, "Unsupported platform, Rhino::HBaseNativeJavaInterface:Scanner requires the JRuby platform" unless RUBY_PLATFORM == "java"

        @htable = htable
        @opts = opts
        @opts[:start_row] ||= ''

        open_scanner
      end

      def open_scanner
        args = []
        args << @opts[:start_row] if @opts[:start_row] != '' || @opts[:stop_row]
        args << @opts[:stop_row] if @opts[:stop_row]

        @scan_criteria = org.apache.hadoop.hbase.client.Scan.new( * (args.map { |v| v.to_java_bytes }))

        if @opts[:columns]
          @opts[:columns].each do |col|
            family, qualifier = col.split(':', 2)

            unless(qualifier.nil? || qualifier.empty?)
              @scan_criteria.addColumn(family.to_java_bytes, qualifier.to_java_bytes)
            else
              @scan_criteria.addFamily(family.to_java_bytes)
            end
          end
        end

        @scan_criteria.setFilter(org.apache.hadoop.hbase.filter.PrefixFilter.new(@opts[:starts_with_prefix].to_java_bytes)) if @opts[:starts_with_prefix]

        @scanner = @htable.get_table.getScanner(@scan_criteria)
      end

      def close_scanner
        @scanner.close()
      ensure
        @scanner = nil
      end

      def next_row
        rowresult = @scanner.next()
        return close_scanner() if rowresult.nil?

        row = @htable.prepare_rowresult(rowresult)
        return row
      rescue Java::IOException
        return close_scanner()
      end

      def each
        while row = next_row()
          yield(row)
        end
      end

      def get_list(nb_rows)
        open_scanner()
        list = []
        nb_rows.times { list << next_row() }
        close_scanner()
        return list
      end
    end
  end
end
