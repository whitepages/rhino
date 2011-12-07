require 'bundler'
require 'gemtools/rake_task'

Bundler::GemHelper.install_tasks
Gemtools::RakeTask.install_tasks

Dir['tasks/**/*.rake'].each { |rake| load rake }
