require File.expand_path('../lib/em-redis/version.rb', __FILE__)

Gem::Specification.new do |s|
  s.name = "em-redis"
  s.version = EMRedis::VERSION
  s.authors = ['Jonathan Broad', 'Eugene Pimenov']
  s.email = 'libc@me.com'
  s.homepage = 'http://github.com/libc/em-redis'

  s.files = Dir['lib/**/*', '*.txt']
  s.require_paths = ["lib"]
  s.summary = "An eventmachine-based implementation of the Redis protocol"
  s.description = s.summary

  s.add_dependency "eventmachine"
  s.add_development_dependency "bundler", "~>1.0.rc.6"
end
