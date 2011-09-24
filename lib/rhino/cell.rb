module Rhino
  class Cell
    attr_reader :key, :contents
    attr_accessor :proxy

    include ActiveModel::Validations
    
    def initialize(key, contents, proxy = nil)
      # Don't set these through #key= and #contents= because those go into Rhino::Model and change the data,
      # but this data is coming directly from Rhino::Model and is thus up-to-date already.
      @key = key
      @contents = contents
      @proxy = proxy
    end
    
    def row
      return nil if self.proxy.nil?
      self.proxy.row
    end
    
    def ==(cell2)
      return false if cell2.nil?
      return false if !row.nil? && !cell2.row.nil? && row != cell2.row
      
      key == cell2.key && contents == cell2.contents
    end
    
    # The full column name of this object in its containing row.
    def attr_name
      "#{self.proxy.column_family_name}:#{key}"
    end
        
    def key=(new_key)
      debug("#{self.class.name}#key= called, going to move value from #{attr_name} to :#{new_key}")
      row.delete_attribute(attr_name)
      @key = new_key
      # after setting @key, attr_name will have changed to the new full column name, so #write will write the right cell
      write 
    end
    
    # Sets the contents of this object and updates them in the containing model.
    def contents=(new_contents)
      debug("#{self.class.name}#contents= called, going to update containing model's #{attr_name}")
      @contents = new_contents
      write
    end
    
    # Writes this cell's key and contents to its row object, but does not save this cell.
    def write
      raise ConstraintViolation, "#{self.class.name} failed constraint #{self.errors.full_messages}" if !self.valid?
      
      row.set_attribute(attr_name, contents)
    end
    
    # Writes this cell's data to the row and saves only this cell.
    def save(timestamp=nil)
      write
      row.class.table.put(row.key, {attr_name=>contents}, timestamp)
    end

    def delete
      row.delete_attribute(attr_name)
    end
    
    # TODO: update to destroy the cell without re-saving the row
    def destroy
      delete
      row.save
    end
    
    def self.belongs_to(containing_class_name)
      debug("#{self.class.name} belongs_to #{containing_class_name}")
      # for the Page example, this would define Cell#page
      define_method(containing_class_name) { row }
    end
  end
end
