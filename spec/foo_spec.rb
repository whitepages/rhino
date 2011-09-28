require "rubygems"
require "rspec"
require File.expand_path(File.dirname(__FILE__) + "/../lib/rhino")

puts "Model = #{Rhino::Model.column_families.inspect}"

class Foo < Rhino::Model
  column_family :apple
end

puts "Model2 = #{Rhino::Model.column_families.inspect}"
puts "Foo = #{Foo.column_families.inspect}"

class Bar < Rhino::Model
  column_family :pear
end

puts "Model2 = #{Rhino::Model.column_families.inspect}"
puts "Foo = #{Foo.column_families.inspect}"
puts "Bar = #{Bar.column_families.inspect}"
