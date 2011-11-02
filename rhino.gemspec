# -*- encoding: utf-8 -*-

# The library's version will always match the version of HBase that the thrift file came from.
module Rhino
  VERSION = "0.2.0"
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

  s.add_dependency "thrift", "~>0.6.0"
  s.add_dependency "activesupport", "~> 3.1.0"
  s.add_dependency "activemodel", "~> 3.1.0"
  s.add_dependency "i18n", "~> 0.6.0"
  
  s.add_dependency "hbase-thrift", "~>0.90.4"
  
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rake', '0.8.7'

  s.files        = Dir.glob("lib/**/*") + %w(CHANGELOG INSTALL MIT-LICENSE README)
  s.require_paths = ['lib']
end
