require File.dirname(__FILE__) + '/spec_helper.rb'

describe Rhino::JsonCell do
  before(:all) do
    Recipe.delete_table if Recipe.table_exists?
    Recipe.create_table
  end
  
  after(:all) do
    Recipe.delete_table
  end

  describe "when working with a has_many relationship using JSON" do
    it "should be be able to serialize without saving" do
      ingredient = Ingredient.new( 'a', :name => 'applesauce', :unit => 'cup', :amount => 1.75 )
      ingredient.name.should == 'applesauce'
      json = ingredient.serializable_hash
      json.should_not == nil

      ingredient1 = Ingredient.new( 'b', json )
      ingredient1.name.should == 'applesauce'
      ingredient1.key.should == 'b'
    end

    it "should be able to create and manage JSON cells" do
      recipe = Recipe.create( 'recipe_0001',
                              :name => 'Pancakes',
                              :ingredients => [ Ingredient.new( 'flour',
                                                                :name => 'all-purpose flour',
                                                                :unit => 'cup',
                                                                :amount => 1.5),
                                                Ingredient.new( 'baking_powder',
                                                                :name => 'baking powder',
                                                                :unit => 'teaspoon',
                                                                :amount => 3.5),
                                                Ingredient.new( 'egg',
                                                                :name => 'egg',
                                                                :amount => 1) ] )
      recipe.ingredients.length.should == 3
      recipe.ingredients.find('flour').name.should == 'all-purpose flour'
      recipe.ingredients.find('baking_powder').amount.should == 3.5      
      recipe.ingredients.find('egg').unit.should == nil
      recipe.save

      check_recipe = Recipe.get( 'recipe_0001' )
      check_recipe.name.should == 'Pancakes'
      check_recipe.ingredients.length.should == 3
      check_recipe.ingredients.find('flour').name.should == 'all-purpose flour'
      check_recipe.ingredients.find('baking_powder').amount.should == 3.5      
      check_recipe.ingredients.find('egg').unit.should == nil
      
      recipe.ingredients << Ingredient.new( 'salt',
                                            :name => 'salt',
                                            :unit => 'teaspoon',
                                            :amount => 1)                                              
      recipe.save

      check_recipe = Recipe.get( 'recipe_0001' )
      check_recipe.ingredients.length.should == 4
      check_recipe.ingredients.find('salt').name.should == 'salt'

      recipe.ingredients.delete_if { |ingredient| ingredient.key == 'egg' }
      recipe.save

      check_recipe = Recipe.get( 'recipe_0001' )
      check_recipe.ingredients.length.should == 3
      check_recipe.ingredients.find('egg').should == nil
      check_recipe.ingredients.find('salt').name.should == 'salt'
    end

    it "should be able to apply constraints to attributes of JSON cells" do
      ingredient = Ingredient.new( 'a', :name => 'a', :unit => 'cup', :amount => 1.75 )
      ingredient.valid?.should == false
     
      ingredient = Ingredient.new( 'a', :name => 'applesauce', :unit => 'cups', :amount => 1.75 )
      ingredient.valid?.should == false

      ingredient = Ingredient.new( 'a', :name => 'applesauce', :unit => 'cup' )
      ingredient.valid?.should == false

      ingredient = Ingredient.new( 'a', :name => 'applesauce', :unit => 'cup', :amount => 1.75 )
      ingredient.valid?.should == true

      ingredient = Ingredient.new( 'a', :name => 'egg', :amount => 1 )
      ingredient.valid?

      value = ingredient.read_attribute_for_validation('unit')
      ingredient.valid?.should == true
      
      recipe = Recipe.create( 'recipe_0002',
                              :name => 'Pancakes',
                              :ingredients => [ Ingredient.new( 'flour',
                                                                :name => 'all-purpose flour',
                                                                :unit => 'cup',
                                                                :amount => 1.5),
                                                Ingredient.new( 'baking_powder',
                                                                :name => 'baking powder',
                                                                :unit => 'teaspoon',
                                                                :amount => 3.5),
                                                Ingredient.new( 'egg',
                                                                :name => 'egg',
                                                                :amount => 1) ] )
      recipe.ingredients.length.should == 3
      recipe.ingredients.find('flour').name.should == 'all-purpose flour'
      recipe.ingredients.find('baking_powder').amount.should == 3.5      
      recipe.ingredients.find('egg').unit.should == nil
      recipe.ingredients.find('flour').valid?.should == true
      recipe.valid?.should == true
      recipe.save
      
      recipe.ingredients.find('flour').amount = nil?
      recipe.ingredients.find('flour').valid?.should == false
      lambda { recipe.ingredients.find('flour').write }.should raise_error(Rhino::ConstraintViolation)
      recipe.valid?.should == false
      lambda { recipe.save }.should raise_error(Rhino::ConstraintViolation)

      recipe = Recipe.new( 'recipe_0003',
                           :name => 'Pancakes',
                           :ingredients => [ Ingredient.new( 'flour',
                                                             :name => 'all-purpose flour',
                                                             :unit => 'cup' ),
                                             Ingredient.new( 'baking_powder',
                                                             :name => 'baking powder',
                                                             :unit => 'teaspoon',
                                                             :amount => 3.5),
                                             Ingredient.new( 'egg',
                                                             :name => 'egg',
                                                             :amount => 1) ] )
      recipe.valid?.should == false
      lambda { recipe.save }.should raise_error(Rhino::ConstraintViolation)      

      lambda do
        recipe = Recipe.create( 'recipe_0004',
                                :name => 'Pancakes',
                                :ingredients => [ Ingredient.new( 'flour',
                                                                  :name => 'all-purpose flour',
                                                                  :unit => 'cup' ),
                                                  Ingredient.new( 'baking_powder',
                                                                  :name => 'baking powder',
                                                                  :unit => 'teaspoon',
                                                                  :amount => 3.5),
                                                  Ingredient.new( 'egg',
                                                                  :name => 'egg',
                                                                  :amount => 1) ] )
      end.should raise_error(Rhino::ConstraintViolation)
    end
  end
end
