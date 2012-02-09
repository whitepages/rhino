require File.dirname(__FILE__) + '/spec_helper.rb'

class JsonCellTest < Rhino::JsonCell

  define_attribute :int_attr,       :type => Integer
  define_attribute :float_attr,     :type => Float
  define_attribute :date_attr,      :type => Date
  define_attribute :date_time_attr, :type => DateTime
  define_attribute :bool_attr,      :type => Rhino::Boolean
  define_attribute :str_attr

end

class StrictJsonCellTest < JsonCellTest
  @table_name = 'json_cell_test'
  
  set_strict( true )
end

class AttrDefns < Rhino::Model
  column_family :tests
  
  has_many :tests, JsonCellTest
end

describe Rhino::AttrDefinitions do
  before(:all) do
    AttrDefns.delete_table if AttrDefns.table_exists?
    AttrDefns.create_table
  end
  
  after(:all) do
    AttrDefns.delete_table
  end

  describe "when working with a has_many relationship" do

    it "should be able to convert at a low level" do
      test = JsonCellTest.new( 'test_0001' )
      test.convert_attribute( :int_attr, "1").should == 1
      test.convert_attribute( :float_attr, "1").should == 1.0
      test.convert_attribute( :float_attr, "1.5").should == 1.5
      test.convert_attribute( :date_attr, "2011-09-25").should == Date.parse( "2011-09-25" )
      test.convert_attribute( :date_time_attr, "2011-09-25T12:40:36-07:00").should == DateTime.parse("2011-09-25T12:40:36-07:00")
      
    end
    
    it "should be able to do trivial serialization" do
      today = Date.today
      now = DateTime.now
      
      test = JsonCellTest.new( 'test_0001' )
      test.int_attr = 1
      test.float_attr = 1.5
      test.date_attr = today
      test.date_time_attr = now
      test.bool_attr = true

      test2 = JsonCellTest.new( 'foo', test.to_json )
      test2.int_attr.should be_a Integer
      test2.int_attr.should == 1
      test2.float_attr.should be_a Float
      test2.float_attr.should == 1.5
      test2.bool_attr.should be_a Rhino::Boolean
      test2.bool_attr.should == true
      test2.date_attr.should be_a Date
      test2.date_attr.should == today
      test2.date_time_attr.should be_a DateTime
      test2.date_time_attr.should === now
    end

    it "should be able to convert a loaded int type" do
      attrd = AttrDefns.create( 'test_0001',
                                 :tests => [ JsonCellTest.new( 'cell_0000',
                                                               :int_attr => 1 ) ] )
      attrd.tests.find('cell_0000').int_attr.should == 1

      check_attrd = AttrDefns.find( 'test_0001' )
      check_attrd.tests.find('cell_0000').int_attr.should == 1
    end

    it "should be able to validate an int attribute" do
      lambda do
        AttrDefns.create( 'test_0001a',
                          :tests => [ JsonCellTest.new( 'cell_0000',
                                                        :int_attr => 'foo' ) ] )
      end.should raise_error(Rhino::TypeViolation)
      
      attrd = AttrDefns.new( 'test_0001b',
                             :tests => [ JsonCellTest.new( 'cell_0000' ) ] )
      lambda do
        attrd.tests.find('cell_0000').int_attr = "foo"
      end.should raise_error(Rhino::TypeViolation)
    end
    
    it "should be able to convert a loaded float type" do
      attrd = AttrDefns.create( 'test_0002',
                                :tests => [ JsonCellTest.new( 'cell_0000',
                                                              :float_attr => 1.5 ) ] )
      attrd.tests.find('cell_0000').float_attr.should == 1.5
      
      check_attrd = AttrDefns.find( 'test_0002' )
      check_attrd.tests.find('cell_0000').float_attr.should == 1.5
    end
    
    it "should be able to validate an float attribute" do
      lambda do
        AttrDefns.create( 'test_0002a',
                          :tests => [ JsonCellTest.new( 'cell_0000',
                                                        :float_attr => 'foo' ) ] )
      end.should raise_error(Rhino::TypeViolation)
      
      attrd = AttrDefns.new( 'test_0002b',
                             :tests => [ JsonCellTest.new( 'cell_0000' ) ] )
      lambda do
        attrd.tests.find('cell_0000').float_attr = "foo"
      end.should raise_error(Rhino::TypeViolation)
    end
    
    it "should be able to convert a loaded date type" do
      today = Date.today

      attrd = AttrDefns.create( 'test_0003',
                                 :tests => [ JsonCellTest.new( 'cell_0000',
                                                               :date_attr => today ) ] )
      attrd.tests.find('cell_0000').date_attr.should be_a Date
      attrd.tests.find('cell_0000').date_attr.should == today
      
      check_attrd = AttrDefns.find( 'test_0003' )
      check_attrd.tests.find('cell_0000').date_attr.should be_a Date
      check_attrd.tests.find('cell_0000').date_attr.should == today
    end

    it "should be able to validate an date attribute" do
      lambda do
        AttrDefns.create( 'test_0003a',
                          :tests => [ JsonCellTest.new( 'cell_0000',
                                                        :date_attr => 'foo' ) ] )
      end.should raise_error(Rhino::TypeViolation)
      
      attrd = AttrDefns.new( 'test_0003b',
                             :tests => [ JsonCellTest.new( 'cell_0000' ) ] )
      lambda do
        attrd.tests.find('cell_0000').date_attr = "foo"
      end.should raise_error(Rhino::TypeViolation)
    end

    it "should be able to convert a loaded datetime type" do
      now = DateTime.now

      attrd = AttrDefns.create( 'test_0004',
                                 :tests => [ JsonCellTest.new( 'cell_0000',
                                                               :date_time_attr => now ) ] )
      attrd.tests.find('cell_0000').date_time_attr.should be_a DateTime
      attrd.tests.find('cell_0000').date_time_attr.should == now
      
      check_attrd = AttrDefns.find( 'test_0004' )
      check_attrd.tests.find('cell_0000').date_time_attr.should be_a DateTime
      check_attrd.tests.find('cell_0000').date_time_attr.should === now
    end

    it "should be able to validate an datetime attribute" do
      lambda do
        AttrDefns.create( 'test_0004a',
                          :tests => [ JsonCellTest.new( 'cell_0000',
                                                        :date_time_attr => 'foo' ) ] )
      end.should raise_error(Rhino::TypeViolation)
      
      attrd = AttrDefns.new( 'test_0004b',
                             :tests => [ JsonCellTest.new( 'cell_0000' ) ] )
      lambda do
        attrd.tests.find('cell_0000').date_time_attr = "foo"
      end.should raise_error(Rhino::TypeViolation)
    end

    it "should be able to convert a loaded bool type" do
      attrd = AttrDefns.create( 'test_0004',
                                 :tests => [ JsonCellTest.new( 'cell_0000',
                                                               :bool_attr => true ) ] )
      attrd.tests.find('cell_0000').bool_attr.should be_a Rhino::Boolean
      attrd.tests.find('cell_0000').bool_attr.should == true
      
      check_attrd = AttrDefns.find( 'test_0004' )
      check_attrd.tests.find('cell_0000').bool_attr.should be_a Rhino::Boolean
      check_attrd.tests.find('cell_0000').bool_attr.should == true
    end

    it "should be able to validate an bool attribute" do
      lambda do
        AttrDefns.create( 'test_0005a',
                          :tests => [ JsonCellTest.new( 'cell_0000',
                                                        :bool_attr => 'foo' ) ] )
      end.should raise_error(Rhino::TypeViolation)
      
      attrd = AttrDefns.new( 'test_0005b',
                             :tests => [ JsonCellTest.new( 'cell_0000' ) ] )
      lambda do
        attrd.tests.find('cell_0000').bool_attr = "foo"
      end.should raise_error(Rhino::TypeViolation)
    end

    it "should be handle a variety of boolean definitions" do
      ['t', 'true', '1', 'yes', 1, true].each do |bool|
        test_name = "test_0005c-#{bool.to_s}"
        attrd = AttrDefns.create( test_name, :tests => [ JsonCellTest.new( 'cell_0000',
                                                                 :bool_attr => bool ) ] )
        attrd.tests.find('cell_0000').bool_attr.should be_a Rhino::Boolean
        attrd.tests.find('cell_0000').bool_attr.should == true

        check_attrd = AttrDefns.find( test_name )
        check_attrd.tests.find('cell_0000').bool_attr.should be_a Rhino::Boolean
        check_attrd.tests.find('cell_0000').bool_attr.should == true
      end
    end

    it "should be able to convert an str attribute" do
      [ 1, 1.0, Date.today, DateTime.now ].each do |testval|
        attrd = AttrDefns.create( 'test_0006',
                                  :tests => [ JsonCellTest.new( 'cell_0000',
                                                                :str_attr => testval ) ] )
        attrd.tests.find('cell_0000').str_attr.should be_a String

        check_attrd = AttrDefns.find( 'test_0006' )
        check_attrd.tests.find('cell_0000').str_attr.should be_a String
      end
    end
    
    it "should be able to handle additional attributes" do
      attrd = AttrDefns.create( 'test_0007',
                                :tests => [ JsonCellTest.new( 'cell_0000',
                                                              :random => 'fantastic' ) ] )
      attrd.tests.find('cell_0000').random.should == 'fantastic'
      
      check_attrd = AttrDefns.find( 'test_0007' )
      check_attrd.tests.find('cell_0000').random.should == 'fantastic'
    end

    it "should be able to be strict" do
      cell = JsonCellTest.new( 'test0008' )
      cell.strict = true
      cell.strict?.should == true

      lambda { cell.unk_attr = "foo" }.should raise_error(Rhino::UnexpectedAttribute)
    end
      
    it "should be able to be defined as strict" do
      cell = StrictJsonCellTest.new( 'test0009' )
      StrictJsonCellTest.strict?.should == true
      cell.strict?.should == true
      
      cell = StrictJsonCellTest.new( 'test0009' )
      lambda { cell.unk_attr = "foo" }.should raise_error(Rhino::UnexpectedAttribute)

      lambda do
        StrictJsonCellTest.new( 'test0000', :unk_attr => 'foo' )
      end.should raise_error(Rhino::UnexpectedAttribute)
    end
  end
end
  




