class Rhino::CellsProxy
  include Enumerable
  
  attr_accessor :row, :column_family_name, :cell_class
  
  def initialize(row, column_family_name, options)
    debug("CellsProxy#initialize(row, #{column_family_name}, #{cell_class})")
    reset
    self.row = row
    self.column_family_name = column_family_name
    self.cell_class = options[:class]
    @options = options
    load_target
  end

  def length
    load_target unless loaded?
    @target.length
  end
  
  def keys
    row.column_family_keys(@column_family_name).collect { |k| Rhino::ColumnFamily.extract_attr_name(k) }
  end
  
  def first(*args)
    load_target unless loaded?
    @target.first(*args)
  end
  
  def last(*args)
    load_target unless loaded?
    @target.last(*args)
  end

  def [](index)
    load_target unless loaded?
    @target[index]
  end

  def find(pattern = nil)
    load_target unless loaded?
    if pattern
      @target.find do |cell|
        case pattern
        when String
          cell.key == pattern
        when Regexp
          cell.key.match( pattern )
        end
      end
    else
      @target.find { |cell| yield cell }
    end    
  end
  
  alias_method( :get, :find )
  
  def select(pattern = nil)
    load_target unless loaded?
    if pattern
      @target.select { |cell| cell.key.match( pattern ) }
    else
      @target.select { |cell| yield cell }
    end    
  end
  
  # Adds a cell using Cell.add(...) and then saves the row to the database.
  # Returns the cell object.
  # def create(key, contents, timestamp=nil)
  #   cell = add(key, contents)
  #   cell.save(timestamp)
  #   return cell
  # end
  
  # def add(key, contents)
  #   cell = new_cell(key, contents)
  #   self.concat( cell )
  #   return cell
  # end
  
  def each
    @target.each do |cell|
      yield cell
    end
  end

  # Resets the \loaded flag to +false+ and sets the \target to +nil+.
  def reset
    @loaded = false
    @target = Array.new
  end
  
  # Loads the \target if needed and returns it.
  #
  # This method is abstract in the sense that it relies on +find_target+,
  # which is expected to be provided by descendants.
  #
  # If the \target is already \loaded it is just returned. Thus, you can call
  # +load_target+ unconditionally to get the \target.
  #
  def load_target
    return nil unless defined?(@loaded)
    
    if !loaded? and (!@row.new_record?)
      @target = keys.collect { |key| load_cell(key) }
    end
    
    @loaded = true
    @target
#  rescue ActiveRecord::RecordNotFound
#    reset
  end
  
  # Reloads the \target and returns +self+ on success.
  def reload
    reset
    load_target
    self unless @target.nil?
  end
  
  # Has the \target been already \loaded?
  def loaded?
    @loaded
  end
  
  # Asserts the \target has been loaded setting the \loaded flag to +true+.
  def loaded
    @loaded = true
  end

  # Returns the target of this proxy, same as +proxy_target+.
  def target
    @target
  end

  # Sets the target of this proxy to <tt>\target</tt>, and the \loaded flag to +true+.
  def target=(target)
    @target = target
    loaded
  end

  def empty?
    load_target unless loaded?
    @target.empty?
  end
  
  # Add +cells+ to this association.  Returns +self+ so method calls may be chained.  
  # Since << flattens its argument list and inserts each record, +push+ and +concat+ behave identically.
  def <<(*cells)
    result = true
    load_target if @row.new_record?
    
    transaction do
      flatten_deeper(cells).each do |cell_data|
        create_cells( cell_data ) do |cell|
          add_cell_to_target_with_callbacks(cell) do |r|
            # should not save here ever really
            # result &&= cell.save unless @row.new_record?
          end
        end
      end
    end
    
    result && self
  end

  alias_method( :push, :<< )
  alias_method( :concat, :<< )
  alias_method( :add, :<< )

  def delete(*cells)
    remove_cells(cells) do |cells|
      cells.each do |cell|
        @row.delete_attribute(cell.attr_name)
        @target.delete(cell)
      end
    end
  end

  def delete_all
    delete( @target )
  end
  
  def delete_if
    remove_cells(@target.select { |cell| yield cell } ) do |cells|
      cells.each do |cell|
        @row.delete_attribute(cell.attr_name)
        @target.delete(cell)
      end
    end
  end

  def replace( other_array )
    # convert the association in hash form with key => contents to an array of cells
    other_array = other_array.collect { |key, val| new_cell( key, val ) } if other_array.is_a?(Hash)

    load_target
    other   = other_array.size < 100 ? other_array : other_array.to_set
    current = @target.size < 100 ? @target : @target.to_set
    
    transaction do
      delete(@target.select { |v| !other.include?(v) })
      concat(other_array.select { |v| !current.include?(v) })
    end
  end

  def valid?
    result = true
    @target.each do |target|
      result &= target.valid?

      if !target.is_a?( cell_class )
        result = false
        target.errors.add "type", "Expected #{cell_class.name} got #{target.class.name}"
      end
    end
    result
  end

  def errors
    error_msgs = @target.collect { |target| target.errors.full_messages }.compact
    error_msgs.join("\n")
  end
  
  def write_all
    transaction do
      @target.each do |target|
        target.write
      end
    end
  end

  
  private
  def add_cell_to_target_with_callbacks(cell)
    callback(:before_add, cell)
    cell.proxy = self
    cell.write if ! @row.new_record?
    yield(cell) if block_given?
    @target ||= [] unless loaded?
    @target << cell unless @options[:uniq] && @target.include?(cell)
    callback(:after_add, cell)
    cell
  end

  def remove_cells(*cells)
    cells = flatten_deeper(cells).collect do |cell|
      case cell
      when Rhino::Cell
        cell
      when String, Regexp
        select(cell)
      else
        raise "Unexpected type of cell to remove #{cell.inspect}"
      end
    end.flatten.compact

    transaction do
      cells.each { |cell| callback(:before_remove, cell) }
      yield(cells)
      cells.each { |cell| callback(:after_remove, cell) }
    end
  end

  # If necessary, we should use this method to implement some form of eventual
  # consistency pattern for HBase. For now though this isn't necessary because
  # all cell data is stored in the row. This will become necessary if we have
  # inter-row relations
  #
  def transaction(*args)
    yield
  end

  # Array#flatten has problems with recursive arrays. Going one level
  # deeper solves the majority of the problems.
  def flatten_deeper(array)
    array.collect do
      |element| (element.respond_to?(:flatten) && !element.is_a?(Hash)) ? element.flatten : element
    end.flatten
  end

  def create_cells( cell_data )
    case cell_data
    when Rhino::Cell
      yield cell_data
    when Hash
      cell_data.each do |k,v|
        yield new_cell(k, v)
      end
    end
  end

  # Instantiate a new cell object pointing to this proxy's row.
  def new_cell(key, contents)
    cell_class.new(key, contents, self)
  end
  alias_method :create, :new_cell
  
  def load_cell(key)
    # consider nil values as nonexistent, because they could refer to cells that will be deleted on the next #save
    # but haven't (a nil value is the marker that it will be deleted)
    if val = @row.get_attribute("#{column_family_name}:#{key}")
      new_cell(key, val)
    else
      return nil
    end
  end
  
  def callback( message, cell )
  end
end
