module Rhino
  module AttrNames
    def self.included(base)
      base.extend(ClassMethods)
    end
    
    module ClassMethods
      def determine_attribute_name(attr_name)
        debug("   determine_attribute_name(#{attr_name.inspect})")
        
        attr_name = attr_name.to_s
        return nil if !attr_name or attr_name.empty?
        return 'timestamp' if attr_name == 'timestamp'
        
        if self.is_valid_attr_name?(attr_name)
          # it is in 'meta:author'-style and thus already a valid attr name, so no need to change it
          return attr_name
        else
          # it is in 'meta_author'-style, so we need to convert it
          attr_name = underscore_name_to_attr_name(attr_name)
          attr_name = self.dealias(attr_name)
          if is_valid_attr_name?(attr_name)
            return attr_name
          else
            # if it is STILL not a valid name, that means it is referring to something we don't know about
            return nil
          end
        end
      end
      
      # Determines whether <tt>attr_name</tt> is a valid column family or column, or a defined alias.
      def is_valid_attr_name?(attr_name)
        return false if attr_name.nil? or attr_name == "" or !attr_name.include?(':')
                
        column_family, column = attr_name.split(':', 2)
        return column_families.include?(column_family)
      end
    
      # Converts underscored attribute names to the corresponding attribute name.
      # "meta_author" => "meta:author"
      # "meta:author" => "meta:author"
      # "title" => "title:"
      # "title:" => "title:"
      def underscore_name_to_attr_name(uname)
        uname = uname.to_s
      
        column_family, column = uname.split('_', 2)
        if column
          "#{column_family}:#{column}"
        else
          "#{column_family}:"
        end
      end
    end
  end
end
