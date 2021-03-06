module Rhino
  module HBaseThriftInterface
    class Table < Rhino::Interface::Table
      attr_reader :hbase, :table_name

      def initialize(hbase, table_name, opts={})
        @hbase = hbase
        @table_name = table_name
        @opts = opts
      end

      def column_families
        determine_column_families unless @opts[:column_families]
        @opts[:column_families]
      end

      DEFAULT_GET_OPTIONS = {:timestamp => nil, :columns => nil}

      def create_table(column_families)
        column_descriptors = column_families.collect do |family_name|
          column = Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new
          column.name = family_name
          column
        end
        @hbase.createTable( table_name, column_descriptors )
      end

      def exists?
        @hbase.table_names.include?( table_name )
      end

      def delete_table
        @hbase.disableTable( table_name )
        @hbase.deleteTable( table_name )
      end

      def get(*rowkeys)
        opts = rowkeys.extract_options!
        opts = DEFAULT_GET_OPTIONS.merge(opts)

        debug{"#{self.class.name}#get(#{rowkeys.inspect}, #{opts.inspect})"}

        raise(ArgumentError, "get requires a key") if rowkeys.nil? || rowkeys.empty? || rowkeys[0].nil? || rowkeys[0]==''

        columns = Array(opts.delete(:columns)).compact

        timestamp = opts.delete(:timestamp)
        timestamp = timestamp.to_i if timestamp

        begin
          rowresult = if timestamp
                        # This appears to be a bug in the HBase thrift interface. It is apparently
                        # ignoring the timestamp that gets pass in for this version of the method
                        #
                        # hbase.getRowsTs(table_name, rowkeys, timestamp + 1)
                        rowkeys.collect { |rowkey| hbase.getRowTs(table_name, rowkey, timestamp + 1) }.flatten
                      else
                        hbase.getRows(table_name, rowkeys)
                      end
        rescue Apache::Hadoop::Hbase::Thrift::IOError => e
          raise Rhino::Interface::Table::TableNotFound, "Table '#{table_name}' not found while looking for key '#{rowkeys.inspect}'" if !exists?
          raise e
        end

        debug{"   => #{rowresult.inspect}"}

        if rowresult.nil? || rowresult[0].nil?
          raise Rhino::Interface::Table::RowNotFound, "No row found in '#{table_name}' with key '#{rowkeys}'"
        end

        # TODO: handle timestamps on a per-cell level
        return rowresult.collect { |row| prepare_rowresult(row) }
      end

      def scan(opts={})
        Rhino::HBaseThriftInterface::Scanner.new(self, opts)
      end

      def put(key, data, timestamp=nil)
        timestamp = timestamp.to_i if timestamp

        mutations = data.collect do |col,val|
          # if the value is nil, that means we are deleting that cell
          mutation_data = {:column=>col}
          if val.nil?
            mutation_data[:isDelete] = true
          else
            raise(ArgumentError, "column values must be strings or nil") unless val.is_a?(String)
            mutation_data[:value] = val
          end
          Apache::Hadoop::Hbase::Thrift::Mutation.new(mutation_data)
        end

        begin
          if timestamp
            hbase.mutateRowTs(table_name, key, mutations, timestamp)
          else
            hbase.mutateRow(table_name, key, mutations)
          end
        rescue Apache::Hadoop::Hbase::Thrift::IOError => e
          raise Rhino::Interface::Table::TableNotFound, "Table '#{table_name}' not found while mutating key '#{key}'" if !exists?
          raise e
        end
      end

      # Deletes the row at +key+ from the table.
      def delete_row(key, opts = {} )
        if opts[:timestamp]
          hbase.deleteAllRowTs(table_name, key, opts[:timestamp])
        else
          hbase.deleteAllRow(table_name, key)
        end
      end

      # Deletes all of the rows in a table.
      def delete_all_rows(opts = {})
        scan.each do |row|
          delete_row(row['key'], opts)
        end
      end

      # Takes a Apache::Hadoop::Hbase::Thrift::TRowResult instance and returns a hash like:
      # {'title:'=>'Some title', 'timestamp'=>1938711819342}
      def prepare_rowresult(rowresult)
        result_columns = rowresult.columns
        data = {}
        result_columns.each { |name, tcell| data[name] = tcell }

        # consider the timestamp to be the timestamp of the most recent cell
        data['timestamp'] = -1
        result_columns.values.each do |tcell|
          data['timestamp'] = tcell.timestamp if data['timestamp'] < tcell.timestamp
        end
        data['key'] = rowresult.row
        return data
      end

      private
      def determine_column_families
        begin
          # column names are returned like 'title', not 'title:', so we have to add the colon on
          @opts[:column_families] = hbase.getColumnDescriptors(table_name).keys.collect { |col_name| "#{col_name}:" }
        rescue Apache::Hadoop::Hbase::Thrift::IOError => e
          raise Rhino::Interface::Table::TableNotFound, "Table '#{table_name}' not found while getting column descriptors" if !exists?
          raise e
        end
      end
    end
  end
end
