class Rhino::ColumnFamilyProxy
  attr_reader :inner
  
  def initialize(row, column_family_name, options)
    @row = row
    @column_family_name = column_family_name
    @options = options

    @inner = options[:class].load( row, column_family_name )
  end

  def write_all
    opts = {}
    opts[:validate] = @options[:validate] if @options[:validate] != nil

    @inner.write( opts )
  end
  
  def replace( other )
    @inner = case other
             when Hash
               @options[:class].new( other )
             when @options[:class]
               other
             else
               raise Rhino::TypeViolation, "Expected #{options[:class]} or Hash for replace"
             end
    @inner.row = @row
    @inner.column_family_name = @column_family_name
  end
  
  def respond_to?( method )
    if super( method )
      return true
    else
      return @inner.respond_to?(method)
    end
  end
  def method_missing(method, *args)
    @inner.send(method, *args)
  end
end
