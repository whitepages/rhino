require 'ruby-debug'
require '../lib/rhino'

base = Rhino::HBaseNativeInterface::Base.new('d0.hdp.pages', '9090')
puts base.table_names.inspect

table = Rhino::HBaseNativeInterface::Table.new(base, 'contact_graph_qa47.pages-contactlists')
data = table.get('ea61715c-c42d-4ee2-a213-e93a8afbf7e0^SCID^')

puts data.inspect

table = Rhino::HBaseNativeInterface::Table.new(base, 'zz-native-interface-test')

table.create_table([:c1, :c2]) unless table.exists?

putdata = {
  "c1:r1" => "THIS IS SWEET!",
  "c1:r2" => "THIS IS ALSO SWEET!",
  "c2:r3" => nil
}

table.put("SUPER_ROW", putdata)

putdata2 = {
  "c1:r1" => nil,
  "c1:r2" => "UPDATED HOW SWEET THIS IS",
  "c2:r3" => "this is no longer nil",
  "c2:r5" => "huh?"
}

table.put("SUPER_ROW", putdata2)

putdata3 = {
  "c1:r6" => "yo"
}

table.put("SUPER_ROW", putdata3, 0)

data = table.get("SUPER_ROW")

puts data.inspect

table.delete_row("SUPER_ROW")

scanner = table.scan

scanner.each do |row|
  puts row.inspect
end

table.delete_table
