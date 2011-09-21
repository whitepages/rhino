module Rhino
  module Constraints
    def self.included(base)
      base.extend(ClassMethods)
    end
    
    # <b>DEPRECATED:</b> Please use <tt>valid?</tt> instead.
    def check_constraints
      warn "[DEPRECATION] `check_constraints` is deprecated.  Please use `valid?` instead."

      if !self.valid?
        raise ConstraintViolation, "#{self.class.name} failed constraint #{self.errors.full_messages}"
      end
    end
    
    module ClassMethods
      def constraints
        @constraints ||= {}
      end
    
    # <b>DEPRECATED:</b> Please use <tt>ActiveModel::Validations</tt> instead.
      def constraint(name, &logic)
        warn "[DEPRECATION] `constraint` is deprecated.  Please use `ActiveModel::Validations` instead."

        raise "`constraint` is deprecated.  Please use `ActiveModel::Validations` instead."
      end
    end
  end
  class ConstraintViolation < Exception; end
end
