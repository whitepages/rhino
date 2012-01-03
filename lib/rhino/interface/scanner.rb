module Rhino
  module Interface
    class Scanner
      include Enumerable

      def initialize(htable, opts = {})
        raise NotImplementedError
      end

      def next_row
        raise NotImplementedError
      end

      def each
        raise NotImplementedError
      end
    end
  end
end
