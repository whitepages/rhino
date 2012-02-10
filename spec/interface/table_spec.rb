require 'spec/spec_helper.rb'

# TODO these tests should not be dependent on Rhino::Model (Page is a subclass of Model)
describe Rhino::Interface::Table do
  before(:all) do
    Page.delete_table if Page.table_exists?
    Page.create_table

    @page_table = Page.table
  end
  
  after(:all) do
    @page_table.delete_all_rows
    Page.delete_table
  end
  
  describe "when getting rows" do
    it "should raise RowNotFound for nonexistent rows" do
      lambda { @page_table.get('this row key does not exist') }.should raise_error(Rhino::Interface::Table::RowNotFound)
    end
    
    it "should raise ArgumentError if nil or blank key is given" do
      lambda { @page_table.get('') }.should raise_error(ArgumentError)
      lambda { @page_table.get(nil) }.should raise_error(ArgumentError)
    end
    
    it "should get the latest timestamp" do
      ts = (Time.now.to_f * 1000).to_i
      @page_table.put('abc', {'title:'=>'hello1', 'contents:'=>'hello there'}, ts)
      @page_table.put('abc', {'title:'=>'hello2'}, ts+1000)
      @page_table.put('abc', {'title:'=>'hello3'}, ts+5000)
      @page_table.get('abc')[0]['timestamp'].should == ts+5000
    end
    
    it "should get the timestamp" do
      @page_table.put('a99', {'title:'=>'hello2'})
      @page_table.get('a99')[0]['timestamp'].should be_within(100).of((Time.now.to_f * 1000).to_i)
    end
    
    it "should retrieve the row" do
      key = 'hello.com'
      @page_table.put(key, {'title:'=>'howdy'})
      row = @page_table.get(key)[0]
      row.keys.sort.should == %w(key timestamp title:)
      row['title:'].value.should == 'howdy'
    end
  end
  
  describe "when deleting all rows" do
    before do
      @page_table.put('a', {'title:'=>'abc'})
      @page_table.put('b', {'title:'=>'bcd'})
      @page_table.put('c', {'title:'=>'cde'})
    end
    
    it "should remove all rows" do
      @page_table.get('a').should_not == nil
      @page_table.delete_all_rows
      lambda { @page_table.get('a') }.should raise_error(Rhino::Interface::Table::RowNotFound)
      @page_table.scan.collect.should == []
    end
  end
  
  describe "when deleting entire rows" do  
    before do
      @some_key = 'some-key'
      @page_table.put(@some_key, {'title:'=>'abc'})
    end
    
    it "should delete the row" do
      @page_table.get(@some_key).should_not == nil
      @page_table.delete_row(@some_key)
      lambda { @page_table.get(@some_key) }.should raise_error(Rhino::Interface::Table::RowNotFound)
    end
  end
  
  describe "when putting rows" do
    it "should require that column values be strings" do
      lambda { @page_table.put('a', {'title:'=>Object}) }.should raise_error(ArgumentError)
    end
  end
  
  describe "when putting existing rows" do
    it "should delete cells that previously existed if their value is changed to nil" do
      key = 'example.com'
      @page_table.put(key, {'title:'=>'howdy', 'links:com.google'=>'Google'})
      @page_table.get(key)[0].keys.include?('links:com.google').should == true
      # the cell has been deleted
      @page_table.put(key, {'title:'=>'howdy', 'links:com.google'=>nil})
      @page_table.get(key)[0].keys.include?('links:com.google').should == false
    end
  end
  
  describe "when putting new rows" do
    describe "when the row is new" do
      it "should create the row before mutating its values"
    end
    
    it "should update the values" do
      key = 'hi.example.com'
      @page_table.put(key, {'title:'=>'howdy'})
      @page_table.get(key)[0]['title:'].value.should == 'howdy'
      @page_table.put(key, {'title:'=>'goodbye'})
      @page_table.get(key)[0]['title:'].value.should == 'goodbye'
    end
  end
end
