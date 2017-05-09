require File.expand_path(File.dirname(__FILE__) + "/test_helper.rb")
require 'logger'

EM.describe EM::Protocols::Redis, "when cluster is resharding" do
  before do
    @c = EM::Protocols::RedisCluster.new([ {:host => "127.0.0.1", :port => 7000} ], logger: Logger.new(STDOUT))
    @c.set "foo", "a"
    @counter = 0
  end

  should "query the value correctly" do
    # manually resharding cluster: redis-trib.rb reshard 127.0.0.1:7000
    # check if 'MOVED xxx xxxxxx:xxxx' happened, and if redis commands works during MOVED
    EM.add_periodic_timer(1) do
      @c.get("foo") do |rsp|
        rsp.should == 'a'
      end

      @c.incr('counter') { |r| @counter += 1; r.should == @counter }
    end

    EM.add_timer(300) {done}
  end
end
