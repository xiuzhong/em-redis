module EventMachine
  module Protocols
      # errors
      class ParserError < StandardError; end
      class ProtocolError < StandardError; end
      class TimeoutError < StandardError; end
      class ConnectError < StandardError; end
      class RedisError < StandardError; end
  end
end