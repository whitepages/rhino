require File.dirname(__FILE__) + '/spec_helper.rb'

describe "when using attribute aliases" do
  before(:all) do
    Page.delete_table if Page.table_exists?
    Page.create_table
  end
  
  after(:all) do
    Page.delete_table
  end

  it "should read the value of the target" do
    @page = Page.new('some-page')
    @page.meta_author = 'Alice'
    @page.author.should == 'Alice'
  end
  
  it "should set the value of the target" do
    @page = Page.new('some-page')
    @page.author = 'Cindy'
    @page.meta_author.should == 'Cindy'
  end
  
  it "should allow instantiation using attribute aliases" do
    @page = Page.create('some-page', :author=>'Bob', :title=>'a title')
    @page.meta_author.should == 'Bob'
    @page.author.should == 'Bob'
  end
end
