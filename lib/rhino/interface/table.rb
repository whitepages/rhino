module Rhino
  module Interface
    class Table
      def initialize(*args)
        raise NotImplementedError
      end

      def create_table(column_families)
        raise NotImplementedError
      end

      def exists?
        raise NotImplementedError
      end

      def delete_table
        raise NotImplementedError
      end

      def get(*rowkeys)
        raise NotImplementedError
      end

      def scan(opts={})
        raise NotImplementedException
      end

      def put(key, data, timestamp=nil, opts={})
        raise NotImplementedError
      end

      def delete_row(*args)
        raise NotImplementedError
      end

      def delete_all_rows
        raise NotImplementedError
      end

      class RowNotFound < Exception
      end

      class TableNotFound < Exception
      end
    end
  end
end
