module Rhino
  module Associations
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      # Specifying that a model <tt>has_many :links</tt> overwrites the Model#links method to
      # return a proxied array of columns underneath the <tt>links:</tt> column family.
      #
      # options:
      #  - :class
      #  - :uniq
      def has_many(column_family_name, options = { :class => Rhino::Cell })
        
        column_family_name = column_family_name.to_s.gsub(':','')
        options = { :class => options } if options.is_a?(Class)
        
        collection_accessor_methods( column_family_name, options, CellsProxy ) 

        column_family column_family_name
      end

      def has_one(column_family_name, options = { :class => Rhino::ColumnFamily })
        
        column_family_name = column_family_name.to_s.gsub(':','')
        options = { :class => options } if options.is_a?(Class)
        
        collection_accessor_methods( column_family_name, options, Rhino::ColumnFamilyProxy )

        column_family column_family_name, :has_one => true
      end

      def collection_reader_method(name, options, association_proxy_class)
        define_method(name) do |*params|
          force_reload = params.first unless params.empty?
          association = association_instance_get(name)
          
          unless association
            association = association_proxy_class.new(self, name, options)
            association_instance_set(name, association)
          end
          
          association.reload if force_reload        
          association
        end      

        validates_each name do | record, attr, value |
          association = record.send(name)
          record.errors.add attr, association.errors if !association.valid?
        end
        
      end
      
      def collection_accessor_methods(name, options, association_proxy_class, writer = true)
        collection_reader_method(name, options, association_proxy_class)
        
        if writer
          define_method("#{name}=") do |new_value|
            # Loads proxy class instance (defined in collection_reader_method) if not already loaded
            association = send(name)
            association.replace(new_value)
            association
          end
        end
      end
    end
    
    # Gets the specified association instance if it responds to :loaded?, nil otherwise.
    def association_instance_get(name)
      association = instance_variable_get("@#{name}")
      association if association.respond_to?(:loaded?)
    end
    
    # Set the specified association instance.
    def association_instance_set(name, association)
      # todo: this is potentially not threadsafe
      association_list = instance_variable_get("@association_list")
      association_list = [] if association_list.nil?
      association_list << association
      instance_variable_set( "@association_list", association_list )
      
      instance_variable_set("@#{name}", association)
    end

    def write_all_associations
      association_list = instance_variable_get("@association_list")

      association_list.each do |proxy|
        proxy.write_all
      end
    end
  end
end

