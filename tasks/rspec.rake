require 'rspec'
require 'rspec/core/rake_task'

desc "Run specs"
RSpec::Core::RakeTask.new do |t|
  # don't run the interface specs together
  t.pattern = "./spec/*_spec.rb"
  # Put spec opts in a file named .rspec in root
end

