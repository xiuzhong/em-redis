require 'rubygems/package_task'
require 'rubygems/specification'
require File.expand_path('../lib/em_redis_cluster', __FILE__)

task :default => ['redis:test']

spec = eval(File.read('em-redis-cluster.gemspec'))
Gem::PackageTask.new(spec) do |pkg|
  pkg.gem_spec = spec
end

desc "install the gem locally"
task :install => [:package] do
  require version_rb
  sh %{sudo gem install pkg/em-redis-cluster-#{EMRedis::VERSION}}
end


namespace :redis do
  desc "Test em-redis against a live Redis"
  task :test do
    sh "bacon spec/live_redis_protocol_spec.rb spec/redis_commands_spec.rb spec/redis_protocol_spec.rb"
  end
end

# EOF
