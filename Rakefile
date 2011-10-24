require 'bundler'
Bundler::GemHelper.install_tasks

Dir['tasks/**/*.rake'].each { |rake| load rake }


task :spec do
  Dir['tasks/*_spec.rb'].each do |path|
    `bin/rspec #{path}`
  end
end
