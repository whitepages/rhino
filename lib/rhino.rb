$:.unshift File.dirname(__FILE__)

require "rubygems"
require 'active_support'
require 'active_model'

# cherry-pick ActiveSupport modules if we can (throws errors under rails 2.3)
# only need Class.cattr_accessor from ActiveSupport
require 'active_support/core_ext/string'
require 'active_support/core_ext/array/extract_options'
require 'active_support/core_ext/class/attribute_accessors'
require 'active_support/core_ext/class/attribute'
require 'active_support/inflector'

# class Array; include ActiveSupport::CoreExtensions::Array::ExtractOptions; end

require 'rhino/interface/base'
require 'rhino/interface/table'
require 'rhino/interface/scanner'

require 'thrift'
require 'thrift/transport/socket'
require 'thrift/protocol/binary_protocol'
require 'hbase'
require 'rhino/interface/hbase-thrift/base'
require 'rhino/interface/hbase-thrift/table'
require 'rhino/interface/hbase-thrift/scanner'
require 'rhino/interface/hbase-fake/base'
require 'rhino/interface/hbase-fake/table'
require 'rhino/interface/hbase-fake/scanner'

if RUBY_PLATFORM == "java"
  # TODO: build this to reference a maven-based set of hbase jars
  Dir["/usr/lib/hbase/\*.jar"].each { |jar| require jar }
  Dir["/usr/lib/hbase/lib/\*.jar"].each { |jar| require jar }

  require 'rhino/interface/hbase-java/base'
  require 'rhino/interface/hbase-java/table'
  require 'rhino/interface/hbase-java/scanner'
end


require 'rhino/debug'
require 'rhino/constraints'
require 'rhino/associations'
require 'rhino/attributes'
require 'rhino/attr_definitions'
require 'rhino/attr_names'
require 'rhino/scanner'
require 'rhino/aliases'
require 'rhino/column_family'
require 'rhino/column_family_proxy'
require 'rhino/cell'
require 'rhino/cells_proxy'
require 'rhino/merged_associations'
require 'rhino/model'
require 'rhino/json_cell'
require 'rhino/active_record_impersonation'

#when in production, probably want to set RHINO_DEBUG = false in environment.rb
RHINO_DEBUG = true unless defined?(RHINO_DEBUG)
include Rhino::Debug

module Rhino
  
end
