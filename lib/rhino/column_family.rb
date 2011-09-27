module Rhino
  # A table's column families are represented in Rhino as ColumnFamily objects. While column families are explicitly specified on the model with
  # Rhino::Model.column_family, a family's child columns can change from row to row. A ColumnFamily instance lets you see which child columns,
  # if any, are set on a given row.
  #
  # You can access a row's column families as ColumnFamilies by calling <tt>row.<b>column_family_name</b>_family</tt>, where 
  # <tt><b>column_family_name</b></tt> is the name of a column family you defined on the row.
  #
  # For example, if you have a column family <tt>title:</tt>, the following code will print out the name and contents of each child column:
  #   for column_full_name in row.title_family.column_full_names
  #     puts "Value of column '#{column_full_name}' is '#{row.get_attribute(column_full_name)}'"
  #   end
  # Example output:
  #   Value of column 'title:english' is 'Hello'
  #   Value of column 'title:french' is 'Bonjour'
  #   Value of column 'title:spanish' is 'Hola'
  #
  # === Accessing columns directly
  # If you just want to access the value of a child column and you already know its name, you do not need to use this class to introspect the structure
  # of the row. You can just access the value by calling a method on the row that has the same name as the full name of the column, with underscores 
  # replacing colons.
  #   # to access the title:english column
  #   row.title_english # => 'Hello'
  #   # to access the title:spanish column
  #   row.title_spanish # => 'Hola'
  # You may also access the value of the column with <tt>Rhino::Model#get_attribute(column_name)</tt>.
  #   row.get_attribute('title:english') # => 'Hello'
  class ColumnFamily
    attr_accessor :column_family_name
    attr_accessor :row
    
    def ColumnFamily.load( row, column_family_name )
      family = self.new
      family.row = row
      family.column_family_name = column_family_name.to_s

      family.load
      family
    end

    def load
      data = {}
      row.column_family_keys(@column_family_name).each do |full_name|
        data[ ColumnFamily.extract_attr_name(full_name) ] = row.get_attribute( full_name )
      end
      reset
      self.attributes = data
      
      @loaded = true
    end

    # Resets the \loaded flag to +false+ and sets the \data to +nil+.
    def reset
      @loaded = false
      @data = {}
    end
    
    # Has the \data been already \loaded?
    def loaded?
      @loaded
    end
    
    def initialize(data = {} )
      @row = nil
      @column_family_name = nil

      self.attributes = data

      @loaded = true
    end
    
    # Writes this column family's data to it's columns
    def write
      raise ConstraintViolation, "#{self.class.name} failed constraint #{self.errors.full_messages}" if !self.valid?
      attributes.each do |name, value|
        row.set_attribute("#{@column_family_name}:#{name}", value)
      end
    end
    alias_method :write_all, :write
    

    # Returns the full names, including the column family, of each child column. If you only want the second half of the name, with the 
    # family name removed, use +column_names+.
    #   row.column_full_names # => ['title:english', 'title:french', 'title:spanish']
    def column_full_names
      attributes.keys.collect{ |key| "#{@column_family_name}:#{key}"}
    end
    
    # Returns the name of the column not including the name of its family. If you want the full name of the column, including the column
    # family name, use +column_full_names+.
    #   row.column_names # => ['english', 'french', 'spanish']
    def column_names
      attributes.keys
    end

    def attributes
      @data
    end

    def loaded?
    end
    
    def self.belongs_to(containing_class_name)
      debug("#{self.class.name} belongs_to #{containing_class_name}")
      # for the Page example, this would define Cell#page
      define_method(containing_class_name) { row }
    end

    def ColumnFamily.determine_attribute_name(attr_name)
      attr_name
    end

    private
    def ColumnFamily.extract_attr_name(name)
      name.split(':', 2)[1]
    end
    
    ColumnFamily.class_eval do
      include Aliases, Attributes, AttrDefinitions
      
      include ActiveModel::Validations
    end
  end
end
