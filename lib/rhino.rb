$:.unshift File.dirname(__FILE__)

require "rubygems"
require "net/http"
require "erb"
require "xml/libxml"
#require "ruby-hbase"

%w(xml_decoder hbase_table base columns debug version scanner).each { |f| require File.dirname(__FILE__) + "/rhino/#{f}" }

module Rhino
  
end