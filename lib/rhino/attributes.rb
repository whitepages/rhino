module Rhino
  module Attributes
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def route_attribute_call(method)
        method = method.to_s
        
        # find verb (get or set)
        if method[-1] == ?=
          verb = :set
          method = method[0..-2] # remove trailing "="
        else
          verb = :get
        end
        
        attr_name = determine_attribute_name(method)
        return nil unless attr_name
        
        debug("-> route_attribute_call: attr_name=#{attr_name.inspect}, verb=#{verb}")
        return [verb, attr_name]
      end      
    end

    def key=(a_key)
      @key = a_key
    end
    
    def key
      @key
    end
    
    def columns
      @data.keys
    end
    
    def data
      @data
    end
    
    def attributes=(new_attributes, guard_protected_attributes = true)
      return if new_attributes.nil?
      attributes = new_attributes.dup
      attributes.stringify_keys!

      attributes.each do |k, v|
        respond_to?(:"#{k}=") ? send(:"#{k}=", v) : raise(UnknownAttributeError, "unknown attribute: #{k}")
      end
    end
    
    def set_attribute(attr_name, value)
      debug("Attributes#set_attribute(#{attr_name.inspect}, #{value.inspect})")
      @data ||= {}
      @data[attr_name] = value
    end
    
    def get_attribute(attr_name)
      debug("Attributes#get_attribute(#{attr_name.inspect}) => #{data[attr_name].inspect}")
      @data ||= {}
      @data[attr_name]
    end
    
    # If <tt>attr_name</tt> is a column family, nulls out the value. If <tt>attr_name</tt> is a column, removes the column from the row.
    def delete_attribute(attr_name)
      debug("Attributes#delete_attribute(#{attr_name.inspect})")
      set_attribute(attr_name, nil)
    end

    alias :respond_to_without_attributes? :respond_to?
    def respond_to?( method )
      if call_data = self.class.route_attribute_call(method)
        verb, attr_name = *call_data
        case verb
        when :get
          return true if get_attribute(attr_name) != nil
        when :set
          return true
        end
      end
      super
    end
    
    # Attempts to provide access to the data by attribute name.
    #   page.meta # => page.data['meta:']
    #   page.meta_author # => page.data['meta:author']
    def method_missing(method, *args)
      debug("Attributes#method_missing(#{method.inspect}, #{args.inspect})")
      if call_data = self.class.route_attribute_call(method)
        verb, attr_name = *call_data
        case verb
        when :get
          get_attribute(attr_name)
        when :set
          set_attribute(attr_name, args[0])
        end
      else
        super # pass it on to Object#method_missing, which will raise NoMethodError
      end
    end
  end
end
