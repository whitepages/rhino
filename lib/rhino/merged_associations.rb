module Rhino
  class MergedAssociationViolation < Exception; end

  module MergedAssociations
    def self.included(base)
      base.extend(ClassMethods)
    end

    class MergedCellsProxy < Rhino::CellsProxy
      attr_accessor :row, :column_family_name, :ordering

      def initialize(row, column_family_name, options)
        debug("CellsProxy#initialize(row, #{column_family_name}, #{cell_class})")
        reset
        self.row = row
        self.column_family_name = column_family_name
        self.ordering = options[:ordering]
        self.cell_class = options[:class]
        @options = options
        load_target
      end

      def <<(*cells)
        raise MergedAssociationViolation, "Cannot modify merged associations"
      end

      def delete(*cells)
        raise MergedAssociationViolation, "Cannot modify merged associations"
      end

      def delete_if
        raise MergedAssociationViolation, "Cannot modify merged associations"
      end

      def replace
        raise MergedAssociationViolation, "Cannot modify merged associations"
      end

      def write_all
      end

      def target=(target)
        raise MergedAssociationViolation, "Cannot modify merged associations"
      end

      private
      def load_target
        @target = []
        @sources ||= self.ordering.collect { |cf| row.send(cf) }
        @keys ||= @sources.collect { |source| source.keys }.flatten.compact.uniq

        @target ||= {}
        @keys.each do |key|
          new_cell = create_cell( key )
          @target << new_cell if !new_cell.nil?
        end
      end
      
      def create_cell( key )
        contents = @sources.collect { |source| source.find(key) }
        return nil if contents.compact.length == 0

        if contents.first.respond_to?( :merge_cell )
          new_cell = nil
          contents.each do |item|
            if item
              if new_cell.nil?
                new_cell = item.deep_clone
              else
                new_cell = new_cell.merge_cell( item )
              end
            else
              # for now do nothing, should have some mechanism to delete in a merge
            end
          end
          new_cell
        else
          contents.last.clone
        end
      end
    end
    
    module ClassMethods
      def has_merged(column_family_name, options = { })
        
        column_family_name = column_family_name.to_s.gsub(':','')
        options = { :ordering => options } if options.is_a?(Array)
        options[:class] ||= Rhino::JsonCell

        raise "Expected ordering list" if options[:ordering].nil?
        
        merged_collection_accessor_methods( column_family_name, options, MergedCellsProxy ) 
      end

      def merged_collection_accessor_methods(name, options, association_proxy_class)
        define_method("create_#{name}") do |*params|
          association = association_proxy_class.new(self, name, options)

          association_instance_set(name, association)
          association
        end
        
        define_method(name) do |*params|
          force_reload = params.first unless params.empty?
          association = association_instance_get(name)

          association = send("create_#{name}") if association.nil?
          return nil if association.nil?

          association.reload if force_reload        
          association
        end      

        validates_each name do | record, attr, value |
          association = record.send(name)
          if !association.nil? && !association.valid?
            case association.errors
            when String
              record.errors.add attr, association.errors
            else
              record.errors.add attr, association.errors.full_messages.join(", ")
            end
          end
        end
      end
      
    end
  end
end