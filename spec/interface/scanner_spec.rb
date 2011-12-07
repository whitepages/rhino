require 'spec/spec_helper.rb'

describe Rhino::Interface::Scanner do
  before(:all) do
    Page.delete_table if Page.table_exists?
    Page.create_table

    @data = ['com.apple',
      'com.google',
      'com.microsoft',
      'com.yahoo',
      'org.apache',
      'org.apache.hbase',
      'org.apache.thrift',
    ]
    @titles = @data.collect { |c| c.split('.').last }

    @page_table = Page.table
    @data.each {|c|
      t = c.split('.').last
      @page_table.put(c, {'title:'=>t})
    }
  end
  
  after(:all) do
    @page_table.delete_all_rows
    Page.delete_table
  end
  
  describe "scanning all rows" do
    it "should return all rows" do
      rows = @page_table.scan.collect
      rows.collect { |row| row['title:'] }.should == @titles
      rows.collect { |row| row['key'] }.should    == @data
    end
  end
  
  describe "when scanning with only a start row specified" do
    it "should return all rows including and after the start row" do
      rows = @page_table.scan(:start_row=>'com.google')
      rows.collect { |row| row['key'] }.should == @data[1..@data.count]
    end
  end
  
  describe "when scanning with only a stop row specified" do
    it "should return all rows up to but not including the stop row" do
      rows = @page_table.scan(:stop_row=>'com.microsoft')
      rows.collect { |row| row['key'] }.should == @data[0..1]
    end
  end

  describe "when scanning with a start row and a stop row specified" do
    it "should return all rows between the start row (inclusive) and stop row (exclusive)" do
      rows = @page_table.scan(:start_row=>'com.google', :stop_row=>'com.yahoo')
      rows.collect { |row| row['key'] }.should == @data[1..2]
    end
  end
  

  describe "when getting an 'n' limited list" do
    it "should return n rows" do
      scanner = @page_table.scan
      rows = scanner.get_list(2)
      rows.count.should == 2
      rows.collect { |row| row['key'] }.should == @data[0..1]
    end
  end
  
  describe "when no rows in the table exist" do
    before do
      @page_table.delete_all_rows
    end
    
    it "should not raise an error" do
      lambda { @page_table.scan }.should_not raise_error
    end
    
    it "should return an empty array" do
      @page_table.scan.collect.should == []
    end
  end

  describe "when scanning for a particular key prefix" do
    it "should return all rows with that prefix" do
      rows = @page_table.scan(:starts_with_prefix=>'org.apache')
      rows.each { |row|
        row.key.start_with?('org.apache').should == true
      }
    end
  end
  
  if ENV['RUN_HUGE_TEST']
    describe "when many rows exist" do
      before do
        (1..45000).each do |n|
          @page_table.put("item#{n}", {'title:'=>"title#{n}"})
        end
      end
      
      it "should scan all rows without an error" do
        lambda { @page_table.scan.each do |row|
          puts row.title
        end }.should_not raise_error
      end
      
    end
  end
end
