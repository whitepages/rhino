
module Rhino
  module Boolean
  end
  
  module AttrDefinitions
    def self.included(base)
      base.extend(ClassMethods)
    end
    
    def strict=(val)
      @strict = val
    end

    def strict?
      @strict ||= self.class.strict?
      @strict
    end


    def convert_attribute( name, value )
      if self.class.attr_definitions[ name.to_s ].nil?
        raise UnexpectedAttribute, "Unexpected attribute #{name} for strict class #{self.class}" if strict?
        return value
      else
        type = self.class.attr_definitions[ name.to_s ][:type] || String

        if !value.is_a?(type)
          begin
            if type == Integer
            value = Integer(value)
            elsif type == Float
              value = Float(value)
            elsif type == Date
              value = Date.parse(value)
            elsif type == DateTime
              value = DateTime.parse(value)
            elsif type == Rhino::Boolean
              value = case value
                      when 't', 'true', '1', 'yes'
                      true
                      when 'f', 'false', '0', 'no'
                        false
                      else
                        raise ArgumentError
                      end
            elsif type == String
              value = value.to_s
            end
          rescue ArgumentError
            raise Rhino::TypeViolation, "Error converting #{value} to #{type} for field #{name}"
          end  
        end
        if !value.is_a?( type )
          raise TypeViolation, "Invalid type  #{value.class} on attribute #{name} of class #{self.class} expected #{type}"
        end

        return value
      end      
    end
    
    module ClassMethods

      def attr_definitions
        @attr_definitions ||= {}
        @attr_definitions
      end
      
      def set_strict(val)
        @strict = val
      end

      def strict?
        @strict ||= false
        @strict
      end

      def define_attribute( name, options = {} )
        @attr_definitions ||= {}
        @attr_definitions[ name.to_s ] = options
      end
    end
  end
  class TypeViolation < Exception; end
  class UnexpectedAttribute < Exception; end
end

class TrueClass; include Rhino::Boolean; end
class FalseClass; include Rhino::Boolean; end
