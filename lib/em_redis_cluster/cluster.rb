# Copyright (C) 2013 Salvatore Sanfilippo <antirez@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'rubygems'
require_relative 'redis_protocol'
require_relative 'redis_error'
require_relative 'crc16'

module EventMachine
  module Protocols
    class RedisCluster

      RedisClusterHashSlots = 16384
      RedisClusterRequestTTL = 16
      RedisClusterDefaultTimeout = 1

      attr_reader :slots_initialized, :startup_nodes

      def initialize(startup_nodes, opt={})
        @startup_nodes = startup_nodes
        
        @connections = {}
        @opt = opt
        @refresh_table_asap = false
        @slots_initialized = false
        initialize_slots_cache {|r| yield(r) if block_given?}
      end

      def get_redis_link(host, port)
        EM::Protocols::Redis.connect(:host => host, :port => port)
      end

      # Given a node (that is just a Ruby hash) give it a name just
      # concatenating the host and port. We use the node name as a key
      # to cache connections to that node.
      def set_node_name!(n)
        n[:name] ||= "#{n[:host]}:#{n[:port]}"
        n
      end

      # Contact the startup nodes and try to fetch the hash slots -> instances
      # map in order to initialize the @slots hash.
      def initialize_slots_cache
        @slots = Array.new(RedisClusterHashSlots)

        fiber = Fiber.new do
      
          @startup_nodes.each do |n|
            @nodes = []

            r = get_redis_link(n[:host], n[:port])

            r.errback {|e| fiber.resume(nil)}

            r.cluster("slots") {|rsp| fiber.resume(rsp)}

            rsp = Fiber.yield
            r.close_connection

            if rsp.is_a?(Array)
              rsp.each do |r|
                
                ip, port = r[2]
                # somehow redis return "" for the node it's querying
                ip = n[:host] if ip == ""

                node = set_node_name!(host: ip, port: port)
                @nodes << node

                (r[0]..r[1]).each {|slot| @slots[slot] = node}
              end

              populate_startup_nodes
              @refresh_table_asap = false
              @slots_initialized = true

              # Exit the loop as long as the first node replies
              break
            else
              next
            end
          end
          yield(@slots_initialized) if block_given?
        end

        fiber.resume
      end

      # Use @nodes to populate @startup_nodes, so that we have more chances
      # if a subset of the cluster fails.
      def populate_startup_nodes
        # Make sure every node has already a name, so that later the
        # Array uniq! method will work reliably.
        @startup_nodes.each{|n| set_node_name!(n)}
        @nodes.each{|n| @startup_nodes << n}
        @startup_nodes.uniq!
      end

      # Flush the cache, mostly useful for debugging when we want to force
      # redirection.
      def flush_slots_cache
        @slots = Array.new(RedisClusterHashSlots)
      end

      # Return the hash slot from the key.
      def keyslot(key)
        # Only hash what is inside {...} if there is such a pattern in the key.
        # Note that the specification requires the content that is between
        # the first { and the first } after the first {. If we found {} without
        # nothing in the middle, the whole key is hashed as usually.
        s = key.index "{"
        if s
          e = key.index "}",s+1
          if e && e != s+1
            key = key[s+1..e-1]
          end
        end

        RedisClusterCRC16.crc16(key) % RedisClusterHashSlots
      end

      # Return the first key in the command arguments.
      #
      # Currently we just return argv[1], that is, the first argument
      # after the command name.
      #
      # This is indeed the key for most commands, and when it is not true
      # the cluster redirection will point us to the right node anyway.
      #
      # For commands we want to explicitly bad as they don't make sense
      # in the context of cluster, nil is returned.
      def get_key_from_command(argv)
        case argv[0].to_s.downcase
        when "info","multi","exec","slaveof","config","shutdown"
          nil
        else
          # Unknown commands, and all the commands having the key
          # as first argument are handled here:
          # set, get, ...
          argv[1]
        end
      end

      def get_random_connection
        n = @startup_nodes.shuffle.first
        @connections[n[:name]] ||= get_redis_link(n[:host], n[:port])
      end

      # Given a slot return the link (Redis instance) to the mapped node.
      # Make sure to create a connection with the node if we don't have
      # one.
      def get_connection_by_slot(slot)
        n = @slots[slot]

        if n
          set_node_name!(n)
          @connections[n[:name]] ||= get_redis_link(n[:host], n[:port])
        else
          # If we don't know what the mapping is, return a random node.
          get_random_connection
        end
      end

      def get_connection_by_node(n)
        set_node_name!(n)
        @connections[n[:name]] ||= get_redis_link(n[:host], n[:port])
      end

      # Dispatch commands.
      def send_cluster_command(argv)
        
        callback = argv.pop
        callback = nil unless callback.respond_to?(:call)

        ttl = RedisClusterRequestTTL
        asking = false
        conn_for_next_cmd = nil
        try_random_node = false

        fiber = Fiber.new do
          while ttl > 0 do
            ttl -= 1
            key = get_key_from_command(argv)

            raise Redis::ParserError.new("No way to dispatch this command to Redis Cluster.") unless key

            # The full semantics of ASK redirection from the point of view of the client is as follows:
            #   If ASK redirection is received, send only the query that was redirected to the specified node but continue sending subsequent queries to the old node.
            #   Start the redirected query with the ASKING command.
            #   Don't yet update local client tables to map hash slot 8 to B.
            conn_for_next_cmd ||= get_connection_by_slot(keyslot(key))
            conn_for_next_cmd.errback {|e| fiber.resume(e)}
            conn_for_next_cmd.asking if asking
            conn_for_next_cmd.send(argv[0].to_sym, *argv[1..-1]) {|rsp| fiber.resume(rsp) }

            rsp = Fiber.yield

            conn_for_next_cmd = nil
            asking = false

            if rsp.is_a?(StandardError)
              errv = rsp.to_s.split
              if errv[0] == "MOVED" || errv[0] == "ASK"
                newslot = errv[1].to_i
                node_ip, node_port = errv[2].split(":")

                if errv[0] == "ASK"
                  asking = true
                  conn_for_next_cmd = get_connection_by_node(host: node_ip, port: node_port)
                else
                  # Serve replied with MOVED. It's better for us to ask for CLUSTER NODES the next time.
                  @refresh_table_asap = true
                  @slots[newslot] = {host: node_ip, port: node_port.to_i}
                end
              else
                raise "!!! REDIS #{e}"
              end
            else
              callback && callback.call(rsp)
              break
            end
          end

          raise "Too many Cluster redirections? (last error: #{e})" if ttl == 0
          initialize_slots_cache if @refresh_table_asap
        end

        fiber.resume
      end

      # Currently we handle all the commands using method_missing for
      # simplicity. For a Cluster client actually it will be better to have
      # every single command as a method with the right arity and possibly
      # additional checks (example: RPOPLPUSH with same src/dst key, SORT
      # without GET or BY, and so forth).
      def method_missing(*argv, &blk)
        argv << blk
        send_cluster_command(argv)
      end
    end
  end
end
