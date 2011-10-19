require File.dirname(__FILE__) + '/spec_helper.rb'

class Beta < Rhino::JsonCell
  define_attribute :str_val,        :type => String
  define_attribute :date_val,       :type => Date
  define_attribute :time_val,       :type => Time
  define_attribute :datetime_val,   :type => DateTime
  define_attribute :int_val,        :type => Integer
  define_attribute :float_val,      :type => Float
  define_attribute :enum_val,       :type => String

  validates_inclusion_of :enum_val, :in => [ 'A', 'B', 'C' ]
end

class Alpha < Rhino::Model
  has_many :betas,          :class => Beta
  has_many :edit_betas,     :class => Beta, :validate => false
  has_merged :merged_betas, :class => Beta, :ordering => [ :betas, :edit_betas ]
end

describe Rhino::MergedAssociations do
  before do
    Alpha.delete_table if Alpha.table_exists?
    Alpha.create_table
  end
  
  after do
    Alpha.delete_table
  end
  
  it "should be able to merge empty relations" do
    alpha = Alpha.new( 'a1' )
    alpha.betas.length.should == 0
    alpha.edit_betas.length.should == 0
    alpha.merged_betas.length.should == 0
    
    alpha = Alpha.new( 'a1', :betas => { :b1 => { :enum_val => 'A' } } )
    alpha.betas.length.should == 1
    alpha.betas[:b1].enum_val.should == 'A'
    alpha.edit_betas.length.should == 0
    alpha.merged_betas.length.should == 1
    alpha.merged_betas[:b1].enum_val.should == 'A'

    alpha = Alpha.new( 'a1', :edit_betas => { :b1 => { :enum_val => 'A' } } )
    alpha.betas.length.should == 0
    alpha.edit_betas.length.should == 1
    alpha.edit_betas[:b1].enum_val.should == 'A'
    alpha.merged_betas.length.should == 1
    alpha.merged_betas[:b1].enum_val.should == 'A'
  end

  it "should be able to merge all data types" do
    d_now = Date.today
    dt_now = DateTime.now
    t_now = Time.now

    d_then = d_now + 5
    dt_then = dt_now + 5
    t_then = t_now + 5
    
    alpha = Alpha.new( 'a1',
                       :betas => {
                         :b1 => {
                           :str_val => 'able',
                           :int_val => 1,
                           :date_val => d_then,
                           :datetime_val => dt_then,
                           :time_val => t_then,
                           :enum_val => 'A'
                         }
                       },
                       :edit_betas => {
                         :b1 => {
                           :str_val => 'baker',
                           :int_val => 2,
                           :float_val => 2.0,
                           :date_val => d_then,
                           :datetime_val => dt_then,
                           :time_val => t_then,
                           :enum_val => 'B'
                         }
                       } )
    alpha.betas.length.should == 1
    alpha.betas[:b1].enum_val.should == 'A'
    alpha.edit_betas.length.should == 1
    alpha.edit_betas[:b1].str_val.should == 'baker'
    alpha.edit_betas[:b1].int_val.should == 2
    alpha.edit_betas[:b1].float_val.should == 2.0
    alpha.edit_betas[:b1].date_val.should == d_then
    alpha.edit_betas[:b1].datetime_val.should == dt_then
    alpha.edit_betas[:b1].time_val.should == t_then
    alpha.edit_betas[:b1].enum_val.should == 'B'
    alpha.merged_betas.length.should.should == 1
    alpha.merged_betas[:b1].enum_val.should == 'B'
    alpha.merged_betas[:b1].str_val.should == 'baker'
    alpha.merged_betas[:b1].int_val.should == 2
    alpha.merged_betas[:b1].float_val.should == 2.0
    alpha.merged_betas[:b1].date_val.should == d_then
    alpha.merged_betas[:b1].datetime_val.should == dt_then
    alpha.merged_betas[:b1].time_val.should == t_then
    alpha.merged_betas[:b1].enum_val.should == 'B'
  end

  it "should be able to respect validations on merged associations" do    
    d_now = Date.today
    dt_now = DateTime.now
    t_now = Time.now

    d_then = d_now + 5
    dt_then = dt_now + 5
    t_then = t_now + 5

    lambda do
      alpha = Alpha.create( 'a1',
                            :betas => {
                              :b1 => {
                                :str_val => 'able',
                                :int_val => 1,
                                :date_val => d_then,
                                :datetime_val => dt_then,
                                :time_val => t_then,
                                :enum_val => 'A'
                              }
                            },
                            :edit_betas => {
                              :b1 => {
                                :enum_val => 'X'
                              }
                            } )
    end.should raise_error Rhino::ConstraintViolation
    lambda do
      alpha = Alpha.create( 'a1',
                            :betas => {
                              :b1 => {
                                :str_val => 'able',
                                :int_val => 1,
                                :date_val => d_then,
                                :datetime_val => dt_then,
                                :time_val => t_then,
                                :enum_val => 'A'
                              }
                            },
                            :edit_betas => {
                              :b2 => {
                                :str_val => 'baker',
                                :int_val => 2,
                                :float_val => 2.0,
                                :date_val => d_then,
                                :datetime_val => dt_then,
                                :time_val => t_then,
                                :enum_val => 'X'
                              }
                            } )
    end.should raise_error Rhino::ConstraintViolation
  end

  it "should not able to make changes to merged associations" do
    alpha = Alpha.new( 'a1',
                       :betas => {
                         :b1 => {
                           :str_val => 'able',
                           :int_val => 1,
                           :date_val => d_then,
                           :datetime_val => dt_then,
                           :time_val => t_then,
                           :enum_val => 'A'
                         }
                       },
                       :edit_betas => {
                         :b1 => {
                           :str_val => 'baker',
                           :int_val => 2,
                           :float_val => 2.0,
                           :date_val => d_then,
                           :datetime_val => dt_then,
                           :time_val => t_then,
                           :enum_val => 'B'
                         }
                       } )
    alpha.merged_betas.length.should == 1

    lambda do
      alpha.merged_betas << { :b2 => { :enum_val => 'A' }}
    end.should raise_error Rhino::MergedAssociationViolation

    lambda do
      alpha.merged_betas.delete_if { |c| true }
    end.should raise_error Rhino::MergedAssociationViolation

    lambda do
      alpha.merged_betas.delete(:b1)
    end.should raise_error Rhino::MergedAssociationViolation

    lambda do
      alpha.merged_betas.replace( :b2 => { :enum_val => 'A' } )
    end.should raise_error Rhino::MergedAssociationViolation

  end
  
end
