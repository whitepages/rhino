module Rhino
  class JsonCell < Cell

    def initialize( key, contents = {}, proxy = nil)
      contents = ActiveSupport::JSON.decode( contents ) if contents.is_a?( String )

      self.attributes = contents
      super( key, contents, proxy )
    end

    # Writes this cell's key and contents to its row object, but does not save this cell.
    def write
      raise ConstraintViolation, "#{self.class.name} failed constraint #{self.errors.full_messages}" if !self.valid?
      
      row.set_attribute(attr_name, self.to_json)
    end    
    
    # Writes this cell's data to the row and saves only this cell.
    def save(timestamp=nil)
      write
      row.class.table.put(row.key, {attr_name => self.to_json}, timestamp)
    end

    def attributes
      data
    end
    
    def JsonCell.is_valid_attr_name?(attr_name)
      return false if attr_name.nil? or attr_name == "" or attr_name.include?(':')
      return true
    end

    def JsonCell.determine_attribute_name(attr_name)
      attr_name
    end
  end

  JsonCell.class_eval do
    include Aliases, Attributes, AttrDefinitions
    
    include ActiveModel::Validations
    include ActiveModel::Serialization
    include ActiveModel::Serializers::JSON

  end
  JsonCell.include_root_in_json = false
end

