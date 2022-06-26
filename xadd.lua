#!lua name=timer
local function timer_xadd(keys, args)
    return redis.call('XADD', keys[1], 'MAXLEN', '~', 4096, '*', unpack(args))
end
  
redis.register_function('timer_xadd', timer_xadd)