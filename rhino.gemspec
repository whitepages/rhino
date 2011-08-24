# -*- encoding: utf-8 -*-
# lib = File.expand_path('lib/', __FILE__)
# $:.unshift lib unless $:.include?(lib)
#
# require 'hbase_thift/version'

# The library's version will always match the version of HBase that the thrift file came from.
module Rhino
  VERSION = "0.0.1"
end

Gem::Specification.new do |s|
  s.name        = "rhino"
  s.version     = Rhino::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Jay Hoover", "Quinn Slack", "Andrew O'Brien"]
  s.email       = ["jhoover@whitepages.com"]
  s.homepage    = "http://github.com/whitepages/rhino"
  s.summary     = "ORM Interface for HBase."
  s.description = "Rhino is a Ruby object-relational mapping (ORM) for HBase[http://www.hbase.org]."

  s.required_rubygems_version = ">= 1.3.6"

  s.add_development_dependency "rspec"

  s.files        = Dir.glob("lib/**/*") + %w(CHANGELOG INSTALL MIT-LICENSE README)
  s.require_paths = ['lib']
end
