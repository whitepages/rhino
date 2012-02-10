module Rhino
  module AttrNames
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
        
        debug{"-> route_attribute_call: attr_name=#{attr_name.inspect}, verb=#{verb}"}
        return [verb, attr_name]
      end      
    end
  end
end
