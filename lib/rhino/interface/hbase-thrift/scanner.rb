module Rhino
  module HBaseThriftInterface
    class Scanner
      include Enumerable
      
      attr_reader :htable
      
      def initialize(htable, opts={})
        @htable = htable
        @opts = opts
        @opts[:start_row] ||= ''
        #raise @opts[:columns].inspect
        
        open_scanner
      end
      
      def open_scanner
        @scanner = if @opts[:stop_row]
          htable.hbase.scannerOpenWithStop(htable.table_name, @opts[:start_row], @opts[:stop_row], @opts[:columns])
        elsif @opts[:starts_with_prefix]
          htable.hbase.scannerOpenWithPrefix(htable.table_name, @opts[:starts_with_prefix], @opts[:columns])
        else
          htable.hbase.scannerOpen(htable.table_name, @opts[:start_row], @opts[:columns])
        end
      end

      def close_scanner
        htable.hbase.scannerClose(@scanner)
        return nil
      end
      
      # Returns the next row in the scanner in the format specified below. Note that the row key is 'key', not 'key:'.
      #   {'key'=>'the row key', 'col1:'=>'val1', 'col2:asdf'=>'val2'}
      def next_row
        begin
          rowresult = htable.hbase.scannerGet(@scanner)
          return nil if rowresult.nil? || rowresult[0].nil?
          
          row = @htable.prepare_rowresult(rowresult[0])          
          row['key'] = rowresult[0].row
          return row
        rescue Apache::Hadoop::Hbase::Thrift::IOError
          htable.hbase.scannerClose(@scanner)
          return nil
        end
      end
      
      def each
        while row = next_row()
          yield(row)
        end
      end

      # Returns the next (n) rows in the scanner in the format specified below. 
      # advances to the next row
      #   {'key'=>'the row key', 'col1:'=>'val1', 'col2:asdf'=>'val2'}
      def get_list(nb_rows)
        begin
          rowresults = htable.hbase.scannerGetList(@scanner, nb_rows)
          return nil if rowresults.nil? || rowresults[0].nil?
          rows = rowresults.collect { |r|
            @htable.prepare_rowresult(r)
          }
          return rows
        rescue Apache::Hadoop::Hbase::Thrift::IOError
          htable.hbase.scannerClose(@scanner)
          return nil
        end
      end

    end
  end
end
