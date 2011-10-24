require File.dirname(__FILE__) + '/spec_helper.rb'

describe "when using constraints" do
  before(:all) do
    Page.delete_table if Page.table_exists?
    Page.create_table
    blank_title = ""
    @page = Page.new('some-page', {:title=>blank_title, :contents=>"hello"})
  end
  
  after(:all) do
    Page.table.delete_all_rows
    Page.delete_table
  end
  
  it "should still handle legacy check constraints" do
    lambda { @page.check_constraints }.should raise_error(Rhino::ConstraintViolation)
  end
  
  it "should not save objects that violate constraints" do
    lambda { @page.save }.should raise_error(Rhino::ConstraintViolation)
  end
  
  it "should save objects that pass constraints" do
    @page.title = "any title will do"
    lambda { @page.save }.should_not raise_error(Rhino::ConstraintViolation)
  end
  
  describe "when using more than one model" do
    class User < Rhino::Model; end
    
    it "should only apply constraints to the model on which they were declared" do
      lambda { User.create('testuser') }.should_not raise_error(Rhino::ConstraintViolation)
    end
  end
end

