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
      
      def get(key, options = {})
        opts = DEFAULT_GET_OPTIONS.merge(options)
        debug("#{self.class.name}#get(#{key.inspect}, #{options.inspect})")
        
        raise(ArgumentError, "get requires a key") if key.nil? or key==''
      
        columns = Array(opts.delete(:columns)).compact

        timestamp = opts.delete(:timestamp)
        timestamp = timestamp.to_i if timestamp

        begin
          older_timestamps = @rows[key].keys.sort
          last_timestamp = if timestamp.nil?
                             older_timestamps.sort.last
                           else
                             older_timestamps.delete_if { |old_timestamp| old_timestamp >= timestamp }
                             older_timestamps.sort.last
                           end
          return @rows[key][last_timestamp][:current].merge( 'timestamp' => last_timestamp )
        rescue
          raise Rhino::Interface::Table::RowNotFound, "No row found in '#{table_name}' with key '#{key}'"
        end

        debug("   => #{rowresult.inspect}")

        return nil if rowresult[0].nil?
        
        # TODO: handle timestamps on a per-cell level
        return prepare_rowresult(rowresult[0])

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
