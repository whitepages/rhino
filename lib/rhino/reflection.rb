module ActiveRecord
  module Reflection # :nodoc:
    def self.included(base)
      base.extend(ClassMethods)
    end

    # Reflection allows you to interrogate Active Record classes and objects about their associations and aggregations.
    # This information can, for example, be used in a form builder that took an Active Record object and created input
    # fields for all of the attributes depending on their type and displayed the associations to other objects.
    #
    # You can find the interface for the AggregateReflection and AssociationReflection classes in the abstract MacroReflection class.
    module ClassMethods
      def create_reflection(macro, name, options, active_record)
        case macro
        when :has_many
          reflection = AssociationReflection.new(macro, name, options, active_record)
        end
        write_inheritable_hash :reflections, name => reflection
        reflection
      end
    end
  end
end
