module Rhino
  module HBaseFakeInterface
    class Table < Rhino::Interface::Table
      attr_reader :hbase, :table_name, :column_families, :rows
      
      def initialize(hbase, table_name, opts={})
        @rows = {}
        @column_families = []
        @hbase = hbase
        @table_name = table_name
        @opts = opts
      end
      
      def column_families
      end
      
      DEFAULT_GET_OPTIONS = {:timestamp => nil, :columns => nil}

      def create_table(column_families)
        @hbase.create_table( table_name )
        @column_families = column_families 
      end

      def exists?
        @hbase.table_names.include?( table_name )
      end
      
      def delete_table
        @hbase.delete_table( table_name )
        @rows = {}
      end

      def exclude_column?(key, columns)
        return false if columns.nil?
        return false if columns.include?(key)
        key = key.split(':', 2)[0] + ":"
        return false if columns.include?(key)
        return true
      end
      
      def get(*rowkeys)
        opts = rowkeys.extract_options!
        opts = DEFAULT_GET_OPTIONS.merge(opts)
        debug("#{self.class.name}#get(#{rowkeys.inspect}, #{opts.inspect})")
        
        raise(ArgumentError, "get requires a key") if rowkeys.nil? or rowkeys.empty? or rowkeys[0]==''
      
        columns = Array(opts.delete(:columns)).compact
        columns = nil if columns.empty?
        
        timestamp = opts.delete(:timestamp)
        timestamp = timestamp.to_i if timestamp

        output = []
        rowkeys.each do |key|
          begin
            older_timestamps = @rows[key].keys.sort
            last_timestamp = if timestamp.nil?
                               older_timestamps.sort.last
                             else
                               older_timestamps.delete_if { |old_timestamp| old_timestamp > timestamp }
                               older_timestamps.sort.last
                             end
            output << @rows[key][last_timestamp][:current].
              delete_if { |k, v| exclude_column?( k, columns ) }.
              merge( 'timestamp' => last_timestamp )
          rescue
            raise Rhino::Interface::Table::RowNotFound, "No row found in '#{table_name}' with key '#{key}'"
          end
        end
        return output
      end
      
      def scan(opts={})
        Rhino::HBaseFakeInterface::Scanner.new(self, opts)
      end
      
      def put(key, data, timestamp=nil)
        timestamp = (Time.now.to_f * 1000).to_i if timestamp.nil?
        timestamp = timestamp.to_i

        @rows[key] ||= {}
        @rows[key][timestamp] ||= {}
        @rows[key][timestamp][:mutations] = merge_mutations( @rows[key][timestamp][:mutations], data )

        timestamps = @rows[key].keys.sort
        timestamps.delete_if { |timestamp| timestamp < timestamp }

        older_timestamps = @rows[key].keys.sort
        older_timestamps.delete_if { |timestamp| timestamp >= timestamp }
        last_time = older_timestamps.last
        
        timestamps.each do |timestamp|
          current = {}
          current = @rows[key][last_time][:current] if @rows[key][last_time]
          @rows[key][timestamp][:current] = merge_current( current, @rows[key][timestamp][:mutations] )
          last_time = timestamp
        end
      end
      
      # Deletes the row at +key+ from the table.
      def delete_row(key)
        @rows[key] = nil
      end
      
      # Deletes all of the rows in a table.
      def delete_all_rows
        scan.each do |row|
          delete_row(row['key'])
        end
      end

      private
      def merge_mutations( old, new )
        old ||= {}
        old.merge( new )
      end

      private
      def merge_current( current, mutations )
        current ||= {}
        current.merge( mutations ).delete_if {|key, val| val.nil?}
      end
      
    end
  end
end
