require File.expand_path(File.dirname(__FILE__) + "/test_helper.rb")

EM.describe EM::Protocols::RedisCluster, "connected to an cluster" do
  # make sure they cover all nodes
  KEYS = ['f1000', 's2ajkdf', 'q12ads9']
  def keys
    KEYS
  end

  before do
    @c = EM::Protocols::RedisCluster.new([ {:host => "127.0.0.1", :port => 7000} ])
    @c.flushall
  end

  should "be able to set a string value" do
    keys.each do |k|
      @c.set(k, "bar") do |r|
        r.should == "OK"
        done
      end
    end
  end

  should "be able to increment the value of a string" do
    keys.each do |k|
      @c.incr k do |r|
        r.should == 1
      end  
      @c.incr k do |r|
        r.should == 2
        done
      end
    end
  end
  
  should "be able to increment the value of a string by an amount" do
    keys.each do |k|
      @c.incrby k, 10 do |r|
        r.should == 10
        done
      end
    end
  end

  should "be able to decrement the value of a string" do
    keys.each do |k|
      @c.incr k do |r|
        r.should == 1
        @c.decr k do |r|
          r.should == 0
          done
        end
      end
    end
  end

  should "be able to decrement the value of a string by an amount" do
    keys.each do |k|
      @c.incrby k, 20 do |r|
        r.should == 20
      end
      @c.decrby k, 10 do |r|
        r.should == 10
        done
      end
    end
  end

  should "be able to 'lpush' to a nonexistent list" do
    keys.each do |k|
      @c.lpush(k, "bar") do |r|
        r.should == 1
        done
      end
    end
  end

  should "be able to 'rpush' to a nonexistent list" do
    keys.each do |k|
      @c.rpush(k, "bar") do |r|
        r.should == 1
        done
      end
    end
  end

  should "be able to add a member to a nonexistent set" do
    keys.each do |k|
      @c.sadd(k, "bar") do |r|
        r.should == 1
        done
      end
    end
  end

  should "be able to get info about the db as a hash" do
    @c.info do |r|
      r.should.key? :redis_version
      done
    end
  end

end

EM.describe EM::Protocols::Redis, "connected to a cluster containing some simple string-valued keys" do

  before do
    @c = EM::Protocols::RedisCluster.new([ {:host => "127.0.0.1", :port => 7000} ])
    @c.set "a", "b"
    @c.set "{a}:x", "y"
    @c.set "x", "y"
  end

  should "be NOT able to fetch the values of multiple keys cross slots" do
    @c.mget "a", "x" do |r|
      r.should == "CROSSSLOT Keys in request don't hash to the same slot"
      done
    end
  end

  should "be able to fetch the values of multiple keys in same slot" do
    @c.mget "a", "{a}:x" do |r|
      r.should == ["b", "y"]
      done
    end
  end

  should "be able to fetch the values of multiple keys in same slot in a hash" do
    @c.mapped_mget "a", "{a}:x" do |r|
      r.should == {"a" => "b",  "{a}:x" => "y"}
      done
    end
  end

  should "be able to set a value if a key doesn't exist" do
    @c.setnx "a", 'foo' do |r|
      r.should == false
      @c.setnx "zzz", "foo" do |r|
        r.should == true
        done
      end
    end
  end

  should "be able to test for the existence of a key" do
    @c.exists "a" do |r|
      r.should == true
      @c.exists "zzy" do |r|
        r.should == false
        done
      end
    end
  end
  
  should "be able to delete a key" do
    @c.del "a" do |r|
      r.should == true
      @c.exists "a" do |r|
        r.should == false
        @c.del "a" do |r|
          r.should == false
          done
        end
      end
    end
  end

  should "be able to detect the type of a key, existing or not" do
    @c.type "a" do |r|
      r.should == "string"
      @c.type "zzy" do |r|
        r.should == "none"
        done
      end
    end
  end

  should "be NOT able to rename a key to another in different slot" do
    @c.rename "a", "x" do |r|
      r.should == "CROSSSLOT Keys in request don't hash to the same slot"
      done
    end
  end

  should "be able to rename a key to another in same slot" do
    @c.rename "a", "{a}:x" do |r|
      r.should == "OK"
      @c.get "{a}:x" do |r|
        r.should == "b"
        done
      end
    end
  end

  should "be able to rename a key unless it exists" do
    @c.renamenx "a", "{a}:x" do |r|
      r.should == false
      @c.renamenx "a", "{a}:zzz" do |r|
        r.should == true
        @c.get "{a}:zzz" do |r|
          r.should == "b"
          done
        end
      end
    end
  end

end

EM.describe EM::Protocols::Redis, "connected to a cluster containing a list" do

  before do
    @c = EM::Protocols::RedisCluster.new([ {:host => "127.0.0.1", :port => 7000} ])
    @c.flushall
    @c.lpush "foo", "c"
    @c.lpush "foo", "b"
    @c.lpush "foo", "a"
  end

  should "be able to 'lset' a list member and 'lindex' to retrieve it" do
    @c.lset("foo",  1, "bar") do |r|
      @c.lindex("foo", 1) do |r|
        r.should == "bar"
        done
      end
    end
  end

  should "be able to 'rpush' onto the tail of the list" do
    @c.rpush "foo", "d" do |r|
      r.should == 4
      @c.rpop "foo" do |r|
        r.should == "d"
        done
      end
    end
  end

  should "be able to 'lpush' onto the head of the list" do
    @c.lpush "foo", "d" do |r|
      r.should == 4
      @c.lpop "foo" do |r|
        r.should == "d"
        done
      end
    end
  end

  should "be able to 'rpop' off the tail of the list" do
    @c.rpop("foo") do |r|
      r.should == "c"
      done
    end
  end

  should "be able to 'lpop' off the tail of the list" do
    @c.lpop("foo") do |r|
      r.should == "a"
      done
    end
  end

  should "be able to get a range of values from a list" do
    @c.lrange("foo", 0, 1) do |r|
      r.should == ["a", "b"]
      done
    end
  end

  should "be able to 'ltrim' a list" do
    @c.ltrim("foo", 0, 1) do |r|
      r.should == "OK"
      @c.llen("foo") do |r|
        r.should == 2
        done
      end
    end
  end

  should "be able to 'rem' a list element" do
    @c.lrem("foo", 0, "a") do |r|
      r.should == 1
      @c.llen("foo") do |r|
        r.should == 2
        done
      end
    end
  end

  should "be able to detect the type of a list" do
    @c.type "foo" do |r|
      r.should == "list"
      done
    end
  end

end

EM.describe EM::Protocols::Redis, "connected to a db containing two sets" do
  before do
    @c = EM::Protocols::RedisCluster.new([{:host => "127.0.0.1", :port => 7000}])
      @c.flushall

      @c.sadd "foo", "a"
      @c.sadd "foo", "b"
      @c.sadd "foo", "c"
      
      @c.sadd "{foo}:a", "c"
      @c.sadd "{foo}:a", "d"
      @c.sadd "{foo}:a", "e"

      @c.sadd "bar", "c"
      @c.sadd "bar", "d"
      @c.sadd "bar", "e"
   
  end

  should "be able to find a set's cardinality" do
    @c.scard("foo") do |r|
      r.should == 3
      done
    end
  end

  should "be able to add a new member to a set unless it is a duplicate" do
    @c.sadd("foo", "d") do |r|
      r.should == 1 # success
      @c.sadd("foo", "a") do |r|
        r.should == 0 # failure
        @c.scard("foo") do |r|
          r.should == 4
          done
        end
      end
    end
  end

  should "be able to remove a set member if it exists" do
    @c.srem("foo", "a") do |r|
      r.should == 1
      @c.srem("foo", "z") do |r|
        r.should == 0
        @c.scard("foo") do |r|
          r.should == 2
          done
        end
      end
    end
  end

  should "be able to retrieve a set's members" do
    @c.smembers("foo") do |r|
      r.sort.should == ["a", "b", "c"]
      done
    end
  end

  should "be able to detect set membership" do
    @c.sismember("foo", "a") do |r|
      r.should == true
      @c.sismember("foo", "z") do |r|
        r.should == false
        done
      end
    end
  end

  should "be NOT able to find the sets' intersection if they're on different slots" do
    @c.sinter("foo", "bar") do |r|
      r.should == "CROSSSLOT Keys in request don't hash to the same slot"
      done
    end
  end

  should "be able to find the sets' intersection if they're on the same slot" do
    @c.sinter("foo", "{foo}:a") do |r|
      r.should == ["c"]
      done
    end
  end

  should "be able to find and store the sets' intersection" do
    @c.sinterstore("{foo}:intersection", "foo", "{foo}:a") do |r|
      r.should == 1
      @c.smembers("{foo}:intersection") do |r|
        r.should == ["c"]
        done
      end
    end
  end

  should "be NOT able to find the sets' union if they're on different slots" do
    @c.sunion("foo", "bar") do |r|
      r.should == "CROSSSLOT Keys in request don't hash to the same slot"
      done
    end
  end

  should "be able to find the sets' union" do
    @c.sunion("foo", "{foo}:a") do |r|
      r.sort.should == ["a","b","c","d","e"]
      done
    end
  end

  should "be able to find and store the sets' union" do
    @c.sunionstore("{foo}:union", "foo", "{foo}:a") do |r|
      r.should == 5
      @c.smembers("{foo}:union") do |r|
        r.sort.should == ["a","b","c","d","e"]
        done
      end
    end
  end

  should "be able to detect the type of a set" do
    @c.type "foo" do |r|
      r.should == "set"
      done
    end
  end

end

EM.describe EM::Protocols::Redis, "when connection is torn down" do
  before do
    @c = EM::Protocols::RedisCluster.new([ {:host => "127.0.0.1", :port => 7000} ], reconn_timer: 1) 
  end

  should "reconnect automatically" do
    #simulate disconnect
    @c.set('foo', 'a') do
      @c.conn_close('foo')
      EM.add_timer(0.5) do
        @c.any_error?.should == true
        @c.conn_status.class.should == Hash
        @c.conn_status[@c.get_slotname_by_key('foo')].should == false
        @c.conn_error?('foo').should == true

        @c.get('foo') do |r|
          r.should == 'a'
          @c.conn_error?('foo').should == false
        end

        @c.get('non_existing') do |r|
          r.should == nil
          @c.conn_error?('foo').should == false
          done
        end
      end
    end
  end

end

