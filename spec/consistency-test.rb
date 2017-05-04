require File.expand_path(File.dirname(__FILE__) + "/../lib/em_redis_cluster")
require 'eventmachine'
require 'fiber'

class ConsistencyTester
    def initialize(redis)
        @r = redis
        @working_set = 1000
        @keyspace = 10000
        @writes = 0
        @reads = 0
        @failed_writes = 0
        @failed_reads = 0
        @lost_writes = 0
        @not_ack_writes = 0
        @delay = 0
        @cached = {} # We take our view of data stored in the DB.
        @prefix = [Process.pid.to_s,Time.now.usec,@r.object_id,""].join("|")
        @errtime = {}
    end

    def genkey
        # Write more often to a small subset of keys
        ks = rand() > 0.5 ? @keyspace : @working_set
        @prefix+"key_"+rand(ks).to_s
    end

    def check_consistency(key,value)
        expected = @cached[key]
        return if !expected  # We lack info about previous state.
        if expected > value
            @lost_writes += expected-value
        elsif expected < value
            @not_ack_writes += value-expected
        end
    end

    def puterr(msg)
        if !@errtime[msg] || Time.now.to_i != @errtime[msg]
            puts msg
        end
        @errtime[msg] = Time.now.to_i
    end

    def test
        last_report = Time.now.to_i

        fiber = Fiber.new do
            while true
                # Read
                key = genkey

                @r.get(key) do |val|
                    if val.is_a?(StandardError)
                        puterr "Reading: #{val.to_s}"
                        @failed_reads += 1
                    else
                        check_consistency(key, val.to_i)
                        @reads += 1
                    end

                    @r.incr(key) do |val|
                        if val.is_a?(StandardError)
                            puterr "Writing: #{e.to_s}"
                            @failed_writes += 1
                        else
                            @cached[key] = val.to_i
                            @writes += 1
                        end
            
                        if Time.now.to_i != last_report
                            report = "#{@reads} R (#{@failed_reads} err) | " +
                                     "#{@writes} W (#{@failed_writes} err) | "
                            report += "#{@lost_writes} lost | " if @lost_writes > 0
                            report += "#{@not_ack_writes} noack | " if @not_ack_writes > 0
                            last_report = Time.now.to_i
                            puts report
                        end
                        fiber.resume
                    end
                end

                Fiber.yield
            end
        end

        fiber.resume
    end
end

if ARGV.length != 2
  startup_nodes = [{host: '127.0.0.1', port: 6379}]
else
  startup_nodes = [{:host => ARGV[0], :port => ARGV[1].to_i}]
end

EM.run do
  rc = EM::Protocols::RedisCluster.new(startup_nodes, logger: nil) do |r|
    # callback when cluster slots information has been retrieved

    unless r
      p 'Failed to setup redis cluster'
      EM.stop
    end

    tester = ConsistencyTester.new(rc)
    tester.test

  end
end
