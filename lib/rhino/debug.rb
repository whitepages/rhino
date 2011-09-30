module Rhino
  module Debug
    @@enable_debug = false
    
    def self.enable_debug
      @@enable_debug = true
    end
    
    def debug(str)
      puts "\e[33mDEBUG: #{str}\e[0m" if @@enable_debug
    end

    def highlight(str)
      puts "\e[35m**** #{str}\e[0m" if @@enable_debug
    end

    def hie(obj)
      highlight obj.inspect
      exit!
    end

    def hr
      highlight('-'*40)
    end

    def hi(obj)
      highlight(obj.inspect)
    end
  end
end
