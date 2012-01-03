module Rhino
  module HBaseNativeInterface
    class Table < Rhino::Interface::Table
      attr_reader :hbase, :table_name

      def initialize(hbase, table_name, opts = {})

      end

      def create(column_families)

      end

      def get(*args)

      end

      def put(*args)

      end

      def delete(*args)

      end
    end
  end
end
