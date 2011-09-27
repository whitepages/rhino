require File.dirname(__FILE__) + '/spec_helper.rb'

class ValidatingPage < Page
  @table_name = 'page'
  validates_length_of :links, :minimum => 1, :maximum => 5
end

class InfoFamily < Rhino::ColumnFamily
  belongs_to :page

  define_attribute :name,       :type => String
  define_attribute :crawl_date, :type => Date
  define_attribute :page_size,  :type => Integer
end

class ConstraintFamily < InfoFamily
  set_strict(true)
  
  define_attribute :name,       :type => String
  define_attribute :crawl_date, :type => Date
  define_attribute :page_size,  :type => Integer

  validates_presence_of :crawl_date
  validates_length_of :name, :minimum => 5
end

class ColumnFamilyTest < Page
  @table_name = 'cfpage'

  has_one :test_info, InfoFamily
end

class ConstraintFamilyTest < Page
  @table_name = 'cfpage'

  has_one :test_info, ConstraintFamily
end

describe Rhino::Cell do
  describe "when working with a has_one relationship" do
    before do
      ConstraintFamilyTest.delete_table if ConstraintFamilyTest.table_exists?
      ConstraintFamilyTest.create_table
    end
    
    after do
      ConstraintFamilyTest.delete_table
    end
    
    it "should be able to create empty relations" do

      test = ColumnFamilyTest.create( 'page_0100',
                                      :title => 'has one page' )
      test.test_info.should_not == nil
      test.test_info.inner.should be_a InfoFamily
      test.test_info.attributes.size.should == 0

      ColumnFamilyTest.find( 'page_0100' ).test_info.should_not == nil
      ColumnFamilyTest.find( 'page_0100' ).test_info.inner.should be_a InfoFamily
      ColumnFamilyTest.find( 'page_0100' ).test_info.attributes.size.should == 0
    end
    
    it "should be possible to replace a relation with a hash" do
      pending
      test = ColumnFamilyTest.create( 'page_0100a',
                                      :title => 'has one page' )
      test.test_info = {
        :name => 'hi there',
      }
      test.test_info.should_not == nil
      test.test_info.inner.should be_a InfoFamily
      test.test_info.name.should == 'hi there'
    end
    
    it "should be able to create a full relation with hash" do
      today = Date.today
      
      test = ColumnFamilyTest.create( 'page_0101',
                                      :title => 'has one page',
                                      :test_info => {
                                        :name => 'Major Page',
                                        :crawl_date => today } )
      test.test_info.name.should == 'Major Page'
      test.test_info.crawl_date.should === today
      
      ColumnFamilyTest.find( 'page_0101' ).test_info.should_not == nil
      ColumnFamilyTest.find( 'page_0101' ).test_info.inner.should be_a InfoFamily
      ColumnFamilyTest.find( 'page_0101' ).test_info.name.should == 'Major Page'
      ColumnFamilyTest.find( 'page_0101' ).test_info.crawl_date.should === today
    end

    it "should be able to create a full relation with instance" do
      today = Date.today
      
      test = ColumnFamilyTest.create( 'page_0102',
                                      :title => 'has one page',
                                      :test_info => InfoFamily.new ( :name => 'Major Page',
                                                                     :crawl_date => today ) )
      test.test_info.name.should == 'Major Page'
      test.test_info.crawl_date.should === today
      
      ColumnFamilyTest.find( 'page_0102' ).test_info.should_not == nil
      ColumnFamilyTest.find( 'page_0102' ).test_info.inner.should be_a InfoFamily
      ColumnFamilyTest.find( 'page_0102' ).test_info.name.should == 'Major Page'
      ColumnFamilyTest.find( 'page_0102' ).test_info.crawl_date.should === today
    end
    
    it "should be possible to set and retrieve attributes of family" do
      today = Date.today
      
      test = ColumnFamilyTest.create( 'page_0103',
                                      :title => 'has one page',
                                      :test_info => InfoFamily.new ( :name => 'Major Page',
                                                                     :crawl_date => today ) )
      test.test_info.name.should == 'Major Page'
      test.test_info.crawl_date.should === today

      test.test_info.page_size = 10000

      ColumnFamilyTest.find( 'page_0103' ).test_info.page_size.should == nil
      test.save
      ColumnFamilyTest.find( 'page_0103' ).test_info.page_size.should == 10000

      test.test_info.most_common_word = 'butternut'
      ColumnFamilyTest.find( 'page_0103' ).test_info.most_common_word.should == nil
      test.save
      ColumnFamilyTest.find( 'page_0103' ).test_info.most_common_word.should == 'butternut'
    end

    it "should be possible to set constraints on family attributes" do
      today = Date.today
      
      lambda do
        ConstraintFamilyTest.create( 'page_0104',
                                     :title => 'constraint page',
                                     :test_info => {
                                       :name => 'shrt',
                                       :crawl_date => today
                                     })
      end.should raise_error Rhino::ConstraintViolation

      lambda do
        ConstraintFamilyTest.create( 'page_0105',
                                     :title => 'constraint page',
                                     :test_info => {
                                       :name => 'longer name'
                                     })
      end.should raise_error Rhino::ConstraintViolation
      
      test = ConstraintFamilyTest.new( 'page_0106')
      test.title = 'constraint page'
      test.test_info.name = 'shrt'
      test.test_info.crawl_date = today

      lambda do
        test.save
      end.should raise_error Rhino::ConstraintViolation

      test = ConstraintFamilyTest.new( 'page_0106')
      test.title = 'constraint page'
      lambda do
        test.test_info.foo = 'bar'
      end.should raise_error Rhino::UnexpectedAttribute
    end
    
  end

  describe "when working with a has_many relationship" do
    
    before do
      Page.delete_table if Page.table_exists?
      Page.create_table
    end
    
    after do
      Page.delete_table
    end
    
    it "should be able to create empty relations" do
      page = Page.create( 'page_0001',
                              :title => 'Foo Home Page' )
      page.title.should == 'Foo Home Page'
      page.links.length.should == 0
      page.images.length.should == 0
      page.save

      check_page = Page.find( 'page_0001' )
      check_page.title.should == 'Foo Home Page'
      check_page.links.length.should == 0
      check_page.images.length.should == 0      
    end

    it "should be able to create a full relation" do
      page = Page.create( 'page_0002',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ) ] )
      page.links.length.should == 1

      check_page = Page.find( 'page_0002' )
      check_page.title.should == 'Foo Home Page'
      check_page.links.length.should == 1

      page.title.should == 'Foo Home Page'
      page.links.length.should == 1
      page.links[0].contents.should == 'www.google.com'
      page.images.length.should == 0
      page.save

      check_page = Page.find( 'page_0002' )
      check_page.title.should == 'Foo Home Page'
      check_page.links.length.should == 1
      check_page.links[0].contents.should == 'www.google.com'
      check_page.images.length.should == 0      

      page = Page.create( 'page_0003',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' )
                                    ] )
      page.title.should == 'Foo Home Page'
      page.links.length.should == 3
      page.links[0].contents.should == 'www.google.com'
      page.images.length.should == 0
      page.save

      check_page = Page.find( 'page_0003' )
      check_page.title.should == 'Foo Home Page'
      check_page.links.length.should == 3
      check_page.links.find('link1').contents.should == 'www.google.com'
      check_page.links.find('link2').contents.should == 'www.facebook.com'
      check_page.links.find('link3').contents.should == 'www.apple.com'
      check_page.images.length.should == 0
    end

    it "should be able to validate length of relations" do
      page = ValidatingPage.new( 'page_0004',
                                 :title => 'Foo Home Page' )
      page.valid?.should == false
      lambda { page.save }.should raise_error(Rhino::ConstraintViolation)

      lambda do
        page = ValidatingPage.create( 'page_0004a',
                                      :title => 'Foo Home Page' )
      end.should raise_error(Rhino::ConstraintViolation)

      page = ValidatingPage.new( 'page_0005',
                                 :title => 'Foo Home Page',
                                 :links => [ Link.new( 'link1', 'www.google.com' ),
                                             Link.new( 'link2', 'www.facebook.com' ),
                                             Link.new( 'link3', 'www.apple.com' ),
                                             Link.new( 'link4', 'www.whitepages.com' ),
                                             Link.new( 'link5', 'www.bing.com' ),
                                             Link.new( 'link6', 'www.linkedin.com' ),
                                             Link.new( 'link7', 'www.snapvine.com' )
                                           ] )
      page.valid?.should == false
      lambda { page.save }.should raise_error(Rhino::ConstraintViolation)

      page = ValidatingPage.new( 'page_0006',
                                 :title => 'Foo Home Page',
                                 :links => [ Link.new( 'link1', 'www.google.com' ),
                                             Link.new( 'link2', 'www.facebook.com' ),
                                             Link.new( 'link3', 'www.apple.com' )
                                              ] )
      page.valid?.should == true
      lambda { page.save }.should_not raise_error(Rhino::ConstraintViolation)

      lambda do
        page = ValidatingPage.create( 'page_0006a',
                                      :title => 'Foo Home Page',
                                      :links => [ Link.new( 'link1', 'www.google.com' ),
                                                  Link.new( 'link2', 'www.facebook.com' ),
                                                  Link.new( 'link3', 'www.apple.com' )
                                                ] )
      end.should_not raise_error(Rhino::ConstraintViolation)
    end

    it "should be able to validate type of relations" do
      page = Page.new( 'page_0007',
                       :title => 'Foo Home Page',
                       :links => [ Image.new( 'link1', 'www.google.com' ),
                                   Image.new( 'link2', 'www.facebook.com' ),
                                   Image.new( 'link3', 'www.apple.com' )
                                 ] )
      page.valid?.should == false
      lambda { page.save }.should raise_error( Rhino::ConstraintViolation )

      lambda do
        page = Page.create( 'page_0007a',
                            :title => 'Foo Home Page',
                            :links => [ Image.new( 'link1', 'www.google.com' ),
                                        Image.new( 'link2', 'www.facebook.com' ),
                                        Image.new( 'link3', 'www.apple.com' )
                                      ] )
      end.should raise_error( Rhino::ConstraintViolation )
    end

    it "should be able to convert relations from strings" do
      page = Page.create( 'page_0008',
                          :title => 'Foo Home Page',
                          :links => { 'link1' => 'www.google.com' } )
      page.valid?.should == true
      lambda { page.save }.should_not raise_error(ArgumentError)

      page.links.length.should == 1
      page.links[0].contents.should == 'www.google.com'
      page.images.length.should == 0
      page.save

      check_page = Page.find( 'page_0008' )
      check_page.title.should == 'Foo Home Page'
      check_page.links.length.should == 1
      check_page.links[0].contents.should == 'www.google.com'
      check_page.images.length.should == 0      
    end

    it "should be able to add relations" do
      page = Page.create( 'page_0009',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' )
                                    ] )
      page.valid?.should == true
      page.links << Link.new( 'link4', 'www.whitepages.com' )
      page.links.add Link.new( 'link5', 'www.foo.com' )
      page.links.add 'link6' => 'www.bar.com'
      page.links.concat 'link7' => 'www.evil.com'
      page.links.find('link4').contents.should == 'www.whitepages.com'
      page.links.find('link5').contents.should == 'www.foo.com'
      page.links.find('link6').contents.should == 'www.bar.com'
      page.links.find('link7').contents.should == 'www.evil.com'
      page.links.length.should == 7

      page.links << [ Link.new( 'link8', 'www.bird.com' ),
                      Link.new( 'link9', 'www.cat.com' ),
                      Link.new( 'link10', 'www.dog.com' ) ]

      page.links.find('link8').contents.should == 'www.bird.com'
      page.links.find('link9').contents.should == 'www.cat.com'
      page.links.find('link10').contents.should == 'www.dog.com'
      page.links.length.should == 10
    end

    it "should be able to remove relations" do
      page = Page.create( 'page_0010',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' )
                                    ] )
      page.links.delete( 'link1' )
      page.links.length.should == 2
      page.links.find('link1').should == nil
      page.links.find('link2').should_not == nil
      page.save
      
      check_page = Page.find( 'page_0010' )
      check_page.links.length.should == 2
      check_page.links.find('link1').should == nil
      check_page.links.find('link2').should_not == nil

      page = Page.create( 'page_0011',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' ),
                                      Link.new( 'extra_link1', 'www.extra.com' )
                                    ] )
      page.links.delete( /^link/ )
      page.links.length.should == 1
      page.links.find('link1').should == nil
      page.links.find('link2').should == nil
      page.links.find('extra_link1').should_not == nil
      page.save
      check_page = Page.find( 'page_0011' )
      check_page.links.length.should == 1
      
      page = Page.create( 'page_0012',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' ),
                                      Link.new( 'extra_link1', 'www.extra.com' )
                                    ] )
      # delete all even links
      page.links.delete_if { |link| /link([0-9]*)/.match(link.key)[1].to_i % 2 == 0 }
      page.links.length.should == 3
      page.links.find('link1').should_not == nil
      page.links.find('link2').should == nil
      page.links.find('extra_link1').should_not == nil

      page = Page.create( 'page_0013',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' ),
                                      Link.new( 'extra_link1', 'www.extra.com' )
                                    ] )
      page.links.delete_all
      page.links.length.should == 0
      page.save
      check_page = Page.find( 'page_0013' )
      check_page.links.length.should == 0
    end

    it "should be able to replace relations" do
      page = Page.create( 'page_0014',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' )
                                    ] )
      page.links.replace( [ Link.new( 'link4', 'www.apple.com' ),
                            Link.new( 'link5', 'www.orange.com' ),
                            Link.new( 'link6', 'www.banana.com' )
                          ] )
      page.links.find('link1').should == nil
      page.links.find('link2').should == nil
      page.links.find('link3').should == nil
      page.links.find('link4').contents.should == 'www.apple.com'
      page.links.find('link5').contents.should == 'www.orange.com'
      page.links.find('link6').contents.should == 'www.banana.com'
      page.links.length.should == 3
      page.save
      check_page = Page.find( 'page_0014' )
      check_page.links.length.should == 3
      check_page.links.find('link1').should == nil
      check_page.links.find('link2').should == nil
      check_page.links.find('link3').should == nil
      check_page.links.find('link4').contents.should == 'www.apple.com'
      check_page.links.find('link5').contents.should == 'www.orange.com'
      check_page.links.find('link6').contents.should == 'www.banana.com'
    end

    it "should be able to modify relations" do
      page = Page.create( 'page_0015',
                          :title => 'Foo Home Page',
                          :links => [ Link.new( 'link1', 'www.google.com' ),
                                      Link.new( 'link2', 'www.facebook.com' ),
                                      Link.new( 'link3', 'www.apple.com' )
                                    ] )
      page.links.find('link1').contents = 'www.nowhere.com'
      page.links.find('link1').contents.should == 'www.nowhere.com'
      page.links.find('link2').contents.should == 'www.facebook.com'
      page.save
      check_page = Page.find( 'page_0015' )
      check_page.links.length.should == 3
      check_page.links.find('link1').contents.should == 'www.nowhere.com'
      check_page.links.find('link2').contents.should == 'www.facebook.com'
    end

    it "should be able to find relations" do
      page = Page.new( 'page_0016',
                       :title => 'Foo Home Page',
                       :links => [ Link.new( 'link13', 'www.google.com' ),
                                   Link.new( 'link17', 'www.facebook.com' ),
                                   Link.new( 'link20', 'www.wikipedia.org' ),
                                   Link.new( 'link21', 'www.bing.com' ),
                                   Link.new( 'link22', 'www.whitepages.com' ),
                                   Link.new( 'link46', 'www.apple.com' )
                                 ] )
      
      page.links.find( /17$/ ).key.should == 'link17'
      page.links.find( /[26]$/ ).key.should == 'link22'
      page.links.find{ |link| link.contents.match(/\.org$/) }.key.should == 'link20'
      page.links.find( 'link17' ).contents.should == 'www.facebook.com'
    end

    it "should be able to select relations" do
      page = Page.new( 'page_0017',
                       :title => 'Foo Home Page',
                       :links => [ Link.new( 'link13', 'www.google.com' ),
                                   Link.new( 'link17', 'www.facebook.com' ),
                                   Link.new( 'link20', 'www.wikipedia.org' ),
                                   Link.new( 'link21', 'www.bing.com' ),
                                   Link.new( 'link22', 'www.whitepages.com' ),
                                   Link.new( 'link46', 'www.apache.org' )
                                 ] )
      page.links.select( /[24680]$/ ).length.should == 3
      page.links.select( /link1/ ).length.should == 2
      page.links.select{ |link| link.contents.match(/\.org$/) }.length.should == 2
    end
  end
end
