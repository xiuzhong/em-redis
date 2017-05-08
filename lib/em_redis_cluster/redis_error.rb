module EventMachine
  module Protocols
    module Redis
      
      # errors

      class ParserError < StandardError; end
      class ProtocolError < StandardError; end

      class ConnectionError < StandardError; end
      class TimeoutError < StandardError; end

      class RedisError < StandardError
        attr_accessor :code
      end

    end
  end
end