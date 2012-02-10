module Rhino
  # == Specify the structure of your Hbase table
  # To set up the mapping from your Hbase table to Rhino, you must specify the table structure:
  #   class Page < Rhino::Model
  #     column_family :title
  #     column_family :contents
  #     column_family :links
  #     column_family :meta
  #   end
  #
  # == Creating rows
  # Each row must have a row key.
  #   page = Page.new('yahoo.com') # row key is 'yahoo.com'
  #   page.title = "Yahoo!"
  #   page.save
  #
  # You also can specify the data as a hash in the second argument to +new+.
  #   page = Page.new('google.com', {:title=>'Google'})
  #   page.contents = "<h1>Welcome to Google</h1>"
  #   page.save
  #
  # Or you can just save the row to the database immediately.
  #   page = Page.create('microsoft.com', {:title=>'Microsoft', :contents=>'<h1>Hello, we are Microsoft!'})
  #
  # == Retrieving and updating existing rows
  # Currently, you can only retrieve existing rows by key or by both key and timestamp (see below).
  #   page = Page.get('yahoo.com')
  #   page.title = "Yahoo! version 2.0"
  #   page.save
  # 
  # == Setting and retrieving by timestamp
  # When saving rows, you can set a timestamp.
  #   a_week_ago = Time.now - 7 * 24 * 3600
  #   Page.create('google.com', {:title=>'Google, a week ago!'}, {:timestamp=>a_week_ago})
  #
  # When retrieving rows, you can specify an optional timestamp to retrieve a certain version of a row.
  #
  #   a_week_ago = Time.now - 7 * 24 * 3600
  #   a_month_ago = Time.now - 30 * 24 * 3600
  #   
  #   newer_page = Page.create('google.com', {:title=>'newer google'}, {:timestamp=>a_week_ago})
  #   older_page = Page.create('google.com', {:title=>'older google'}, {:timestamp=>a_month_ago})
  #   
  #   # now you can get() by the timestamps you just set
  #   Page.get('google.com', :timestamp=>a_week_ago).title # => "newer google"
  #   Page.get('google.com', :timestamp=>a_month_ago).title # => "older google"
  #
  # If no timestamp is specified when retrieving rows, the most recent row will be returned.
  # 
  #   page = Page.get('google.com')
  #   page.title # => 'newer google'
  #
  # If a timestamp is specified that does not match any rows of that key in the database, <tt>nil</tt> is returned.
  #
  #   three_days_ago = Time.now - 3 * 24 * 3600
  #   Page.get('google.com', :timestamp=>three_days_ago) # => nil
  #
  # == Accessing data on rows
  # A row's attributes may be accessed or written as follows.
  #
  # For column families:
  #
  #   page.title # returns value of title: column
  #   page.title = 'Welcome!' # sets value of title: column
  #
  # For child columns (columns underneath a column family):
  #
  #   page.meta_author # returns value of meta:author column
  #   page.meta_language = 'en-US' # sets value of meta:language column
  class Model

    def initialize(key, data={}, opts={})
      debug{"Model#initialize(#{key.inspect}, #{data.inspect}, #{opts.inspect})"}

      self.key = key
      self.opts = {:new_record=>true}.merge(opts)

      self.attributes = data

    end

    def save( input_opts = {} )
      default_opts = self.class.default_save_opts

      opts = default_opts.clone if !default_opts.nil?
      opts ||= {}
      
      input_opts = { :timestamp => input_opts } if input_opts.is_a? Integer
      opts.merge!( input_opts )
      
      debug{"Model#save() [key=#{key.inspect}, data=#{data.inspect}, timestamp=#{opts[:timestamp].inspect}]"}

      if !self.valid?
        raise ConstraintViolation, "#{self.class.name} failed constraint #{self.errors.full_messages.join("\n")}"
      end
      
      write_all_associations
      
      # we need to delete data['timestamp'] here or else it will be written to hbase as a column (and will
      # cause an error since no 'timestamp' column exists)
      # but we also want to invalidate the timestamp since saving the row will give it a new timestamp,
      # so this accomplishes both
      data.delete('timestamp')

      output = {}
      data.keys.each do |k|
        next if !opts[:include].nil? && !included_column( k, opts[:include] )
        next if !opts[:exclude].nil? && included_column( k, opts[:exclude] )
        output[k] = save_attribute( k )
      end

      self.class.table( opts[:table] ).put(key, output, opts[:timestamp])
      if new_record?
        @opts[:new_record] = false
        @opts[:was_new_record] = true
      end
      return true
    end
    
    def destroy
      debug{"Model#destroy() [key=#{key.inspect}]"}
      self.class.table.delete_row(key)
    end
    
    def new_record?
      @opts[:new_record]
    end
    
    def was_new_record?
      @opts[:was_new_record] || false
    end
    
    # Returns true if the +comparison_object+ is the same object; or is of the same type, has the same key and data, and is not a new record.
    def ==(comparison_object)
      comparison_object.equal?(self) ||
        (comparison_object.instance_of?(self.class) &&
          comparison_object.data == data &&
          comparison_object.key == key &&
          !comparison_object.new_record?)
    end
    
    def timestamp
      @data ||= {}
      @data['timestamp']
    end
    
    def timestamp=(a_timestamp)
      @data ||= {}
      @data['timestamp'] = a_timestamp
    end
    
    def column_family_keys( column_family )
      @data ||= {}
      @data.keys.select { |k| k.match(/^#{Regexp.escape(column_family)}/)}
    end
    
    private
    
    attr_reader :opts
    def opts=(some_opts)
      @opts = some_opts
    end
    
    # Data that is set here must have HBase-style keys (like {'meta:author'=>'John'}), not underscored keys {:meta_author=>'John'}.
    def data=(some_data)
      debug{"Model#data=(#{some_data.inspect})"}
      @data = {}
      some_data.each do |data_key,val|
        attr_name = self.class.determine_attribute_name(data_key)
        raise(ArgumentError, "invalid attribute name for (#{data_key.inspect},#{val.inspect})") unless attr_name
        set_attribute(attr_name, val)
      end
      debug{"Model#data == #{data.inspect}"}
      data
    end

    def included_column( column_name, column_specs )
      return false if column_specs.nil? || column_specs.empty?

      return true if column_specs.include?( column_name )
      return true if column_specs.include?( column_name.split(":").first + ":" )

      return false
    end
        

    #################
    # CLASS METHODS #
    #################
    
    def Model.connect(host, port, adapter=nil)

      unless adapter
        if RUBY_PLATFORM == "java"
          adapter = Rhino::HBaseNativeJavaInterface
        else
          adapter = Rhino::HBaseThriftInterface
        end
      end

      debug{"Model.connect(#{host.inspect}, #{port.inspect}, #{adapter.inspect})"}
      raise "already connected" if connection
      @adapter = adapter
      @conn = adapter::Base.new(host, port)
    end
    
    def Model.adapter
      @adapter
    end
    
    # Returns true if connected to the database, and false otherwise.
    def Model.connected?
      @conn != nil
    end

    def Model.disconnect
      return unless Model.connected?
      debug{"Model.disconnect"}
      @conn.disconnect
      @conn = nil
      @adapter = nil
    end
    
    # Returns the connection. The connection is shared across all models and is stored in Rhino::Model,
    # so models retrieve it from Rhino::Table.
    def Model.connection
      # uses self.name instead of self.class because in class methods, self.class==Object and self.name=="Rhino::Model"
      if self.name == "Rhino::Model"
        @conn
      else
        Rhino::Model.connection
      end
    end
    
    # Returns the table interface.
    def Model.table( input_table_name = nil )
      input_table_name ||= table_name
      @table_cache ||= {}
      @table_cache[ input_table_name ] ||= Rhino::Model.adapter::Table.new(connection, input_table_name)

      return @table_cache[ input_table_name ]
    end

    class_attribute :column_families

    def Model.column_family(name, options = {})
      name = name.to_s.gsub(':','')
      self.column_families ||= []
      if self.column_families.include?(name)
        debug{"column_family '#{name}' already defined for #{self.class.name}"}
        self.column_families.delete(name)
      end
      self.column_families << name

      if options[:has_one] == true
        class_eval %Q{
          def #{name}_family
            #{name}
          end
        }
      else
        class_eval %Q{
          def #{name}_family
            @#{name}_family ||= Rhino::ColumnFamily.load(self, :#{name})
          end
        }
      end

      # also define Model#meta_columns and Model#meta_family methods for each column_family
      class_eval %Q{
        def #{name}_column_names
          #{name}_family.column_names
        end
        
        def #{name}_column_full_names
          #{name}_family.column_full_names
        end
      }

    end
    
    # Determines the table name, even if the model class is within a module (ex: CoolModule::MyThing -> mythings).
    # You can override this by defining the <tt>table_name</tt> class method on your model class.
    def Model.table_name
      prefix = Model.table_name_prefix.nil? ? "" : Model.table_name_prefix + "-"
      
      return prefix + @table_name if !@table_name.nil?
      return prefix + self.name.downcase.split('::')[-1].pluralize
    end

    def Model.table_name=( table_name )
      @table_name = table_name
    end

    cattr_accessor :table_name_prefix
    
    # loads an existing record's data into an object
    def Model.load(key, data)
      new(key, data, {:new_record=>false})
    end
    
    def Model.create(key, data={}, opts={})
      obj = new(key, data, opts)
      obj.save(opts)
      obj
    end

    def Model.create_table( table_name = nil )
      table(table_name).create_table( column_families )
    end

    def Model.delete_table( table_name = nil )
      table(table_name).delete_table
    end

    def Model.table_exists?( table_name = nil )
      table(table_name).exists?
    end    
    
    # Scans the table with +opts+ (if provided) and returns an array of each row that is returned by the scanner.
    # See +scan+ for options.
    def Model.get_all(opts={})
      scan(opts).collect
    end
    
    # Returns a Scanner that will iterate through the rows, according to the arguments.
    # ==== Options
    # * <tt>:start_row => 'row key'</tt> - Return only rows whose keys, in lexical order, occur at or after this row key.
    #   The row specified by the key supplied in <tt>:start_row</tt>, if it exists, will be returned.
    #   This option can be combined with <tt>:stop_row</tt>.
    # * <tt>:stop_row => 'row key'</tt> - Return only rows whose keys, in lexical order, occur before this row key.
    #   The row specified by the key supplied in <tt>:stop_row</tt>, if it exists, will NOT be returned.
    #   This option can be combined with <tt>:start_row</tt>.
    # ==== Notes
    # Note that <tt>:start_row</tt> is inclusive of the start row key, while <tt>:stop_row</tt> is exclusive.
    # For example, with row keys A, B, and C, starting at B would return B and C. If the stop row were C, however,
    # the Scanner would only return A and B.
    def Model.scan(opts={})
      Rhino::Scanner.new(self, opts)
    end

    def Model.get(rowkey, opts = {})
      output = self.find_all( rowkey, opts )
      return nil if output.nil?

      return output[0]
    end

    def Model.get_table_for( table_spec )
      case table_spec
      when String
        return self.table( table_spec )
      when Rhino::Model.adapter::Table
        return table_spec
      end
    end
    
    def Model.find_all(*rowkeys)
      get_opts = rowkeys.extract_options!
      rowkeys.flatten!      

      debug{"Model.get(#{rowkeys.inspect}, #{get_opts.inspect})"}
      
      # handle opts
      get_opts.keys.each do |fo_key|
        unless %w(columns timestamp tables).include?(fo_key.to_s)
          raise ArgumentError, "invalid key for get opts: #{fo_key.inspect}"
        end
      end
      
      base_timestamp = get_opts[:timestamp]

      merge_tables = get_opts[:tables] || [ { :table => table, :timestamp => base_timestamp } ]
      
      row_data = {}
      
      merge_tables.each do |table_spec|
        table, timestamp = case table_spec
                           when Hash
                             [ get_table_for(table_spec[:table]), table_spec[:timestamp] ]
                           else
                             [ get_table_for(table_spec), nil ]
                           end

        timestamp ||= base_timestamp
        
        # get the row
        begin
          args = rowkeys.clone
          opts = { :timestamp => timestamp }
          opts[:columns] = get_opts[:columns] if get_opts[:columns]
          args << opts
          
          data = table.get( *args )
          debug{"-> found [key=#{rowkeys.inspect}, data=#{data.inspect}]"}

          rowkeys.each do |key|
            new_data = data.find {|item| item['key'] == key } 

            row_data[ key ] ||= {}
            row_data[ key ].merge!( new_data ) if !new_data.nil?
          end
        rescue Rhino::Interface::Table::RowNotFound
        end
      end


      output = []

      rowkeys.each do |key|
        if !row_data[key].nil? && row_data[key].size > 0
          row_data[key].delete('key')
          output << load( key, row_data[key] )
        end
      end
      output
    end
 
    class << self
      alias_method( :find, :get )
    end
    
    def Model.delete_all(opts = {})
      table.delete_all_rows(opts)
    end

    def Model.default_save_opts( opts = nil )
      if !opts.nil?
        @save_opts = opts
        @save_opts[:include] = cleanup_column_spec(@save_opts[:include]) if !@save_opts[:include].nil?
        @save_opts[:exclude] = cleanup_column_spec(@save_opts[:exclude]) if !@save_opts[:exclude].nil?
      end
      return @save_opts
    end

    def Model.cleanup_column_spec( columns )
      columns.collect do |column|
        case column
        when Symbol
          column.to_s + ":"
        when String
          column += ":" if !column.include?(":")
          column
        end
      end
    end
  end

  Model.class_eval do
    include Aliases, AttrDefinitions, AttrNames, Attributes, Associations, MergedAssociations

    
    include ActiveModel::Validations
    include ActiveModel::Serialization
  end
end
