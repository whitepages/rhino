module Rhino
  module HBaseFakeInterface
    class Base < Rhino::Interface::Base

      def initialize(host, port)
        @table_names = []
      end
      
      def table_names
        @table_names
      end

      def create_table( table_name )
        @table_names << table_name
      end

      def delete_table( table_name )
        @table_names.delete( table_name )
      end
    end
  end
end
