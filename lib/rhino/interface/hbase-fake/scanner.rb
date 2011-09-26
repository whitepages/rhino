module Rhino
  module HBaseFakeInterface
    class Scanner
      include Enumerable
      
      attr_reader :htable
      
      def initialize(htable, opts={})
        @htable = htable
        @opts = opts
        @opts[:start_row] ||= ''
        #raise @opts[:columns].inspect

        @scan_keys = htable.rows.keys.sort.select do |key|
          key >= @opts[:start_row] && ( @opts[:end_row].nil? || key <= @opts[:end_row] )
        end
      end
      
      # Returns the next row in the scanner in the format specified below. Note that the row key is 'key', not 'key:'.
      #   {'key'=>'the row key', 'col1:'=>'val1', 'col2:asdf'=>'val2'}
      def next_row
        key = @scan_keys.shift
        return nil if key.nil?

        row = @htable.get( key )
        row['key'] = key
        return row
      end
      
      def each
        while row = next_row()
          yield(row)
        end
      end
    end
  end
end


