require File.dirname(__FILE__) + '/spec_helper.rb'

describe Rhino::Scanner do
  before(:all) do
    Page.delete_table if Page.table_exists?
    Page.create_table
    @p1 = Page.create('com.example', :title =>'example')
    @p2 = Page.create('com.google', :title =>'Google', 'links:gov.whitehouse'=>'link1', 'links:gov.change'=>'link2')
    @p3 = Page.create('com.microsoft', :title =>'Microsoft')
    @p4 = Page.create('com.yahoo', :title =>'Yahoo')
    @p5 = Page.create('org.apache', :title =>'apache')
    @p6 = Page.create('org.apache.hbase', :title =>'hbase')
    @p7 = Page.create('org.apache.thrift', :title =>'thrift')
  end

  after(:all) do
    Page.table.delete_all_rows
    Page.delete_table
  end
  
  def column_data_of(rows)
    rows.collect do |row|
      row.data.delete('timestamp')
      row.data.size == 0 ? nil : row.data
    end.compact
  end
  
  describe "when getting all rows" do
    before do
      @all_pages = Page.get_all
    end
    
    it "should return all rows" do
      column_data_of(@all_pages).should == column_data_of([@p1, @p2, @p3, @p4, @p5, @p6, @p7])
    end
  end
  
  describe "when scanning all rows" do
    it "should return all rows" do
      column_data_of(Page.scan.collect).should == column_data_of([@p1, @p2, @p3, @p4, @p5, @p6, @p7])
    end
  end
  
  describe "when scanning with a start row specified" do    
    it "should show rows including and after the start row" do
      column_data_of(Page.scan(:start_row=>'com.google').collect).should == column_data_of([@p2, @p3, @p4, @p5, @p6, @p7])
    end
  end
  
  describe "when scanning with a start row and an stop row specified" do
    it "should return all rows between the start row and stop row (inclusive)" do
      column_data_of(Page.scan(:start_row=>'com.google', :stop_row=>'com.yahoo').collect).should == column_data_of([@p2, @p3])
    end
  end
  
  describe "when scanning with an stop row specified" do
    it "should only show rows up to and including the stop row" do
      column_data_of(Page.scan(:stop_row=>'com.microsoft').collect).should == column_data_of([@p1, @p2])
    end
  end
  
  describe "when scanning only certain columns" do
    it "should only populate the model with those columns' data" do
      column_data_of(Page.scan(:columns=>['links:']).collect).should == [{"links:gov.whitehouse"=>"link1", "links:gov.change"=>"link2"}]
    end
  end

  describe "when scanning for a particular key prefix" do
    it "should return all rows with that prefix" do
debugger
      column_data_of(Page.scan(:starts_with_prefix =>'org.apache').collect).should == column_data_of([@p5, @p6, @p7])
    end
  end
end
