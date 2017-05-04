require File.expand_path('../lib/em_redis_cluster/version.rb', __FILE__)

Gem::Specification.new do |s|
  s.name = "em_redis_cluster"
  s.version = EMRedis::VERSION
  s.authors = ['Jonathan Broad', 'Eugene Pimenov']
  s.email = 'lxz.tty@gmail.com'
  s.homepage = 'https://github.com/xiuzhong/em_redis_cluster'

  s.files = Dir['lib/**/*', '*.txt']
  s.require_paths = ["lib"]
  s.summary = "An eventmachine-based implementation of the Redis protocol and Redis cluster support"
  s.description = s.summary

  s.add_dependency "eventmachine"
  s.add_development_dependency "bundler", "~>1.0.rc.6"
end
