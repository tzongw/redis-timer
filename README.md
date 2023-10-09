# Timer Redis Module

This module allows the delayed execution of LUA scripts, both periodic and one-time.

# Features

1. High performance.
2. Supports persistence, both RDB and AOF.
3. Supports replication and cluster.

## Quick start

1. Obtain a redis server version 7.0 or above (with [function](https://redis.io/docs/manual/programmability/functions-intro/) support)
2. Download and compile redis-timer (`make`)
3. Start the redis server with the timer module:
    - By adding this line to redis.conf: `loadmodule /path/to/timer.so`
    - By using a command-line argument: `redis-server --load-module /path/to/timer.so`
    - By running the command: `MODULE LOAD /path/to/timer.so`


## Commands

### `TIMER.NEW id function milliseconds [LOOP] numkeys [key [key ...]] [arg [arg ...]]`

Create and activate a new timer. Also a value with name `id` will be created in redis db.
```
127.0.0.1:6379> TIMER.NEW id function 10000 LOOP 1 key arg
(integer) 1
127.0.0.1:6379> type id
timer-tzw
```

The `function` will be
executed after `milliseconds` with `numkeys [key [key ...]] [arg [arg ...]]` as arguments via [FCALL](https://redis.io/commands/fcall/). If `LOOP` is specified, after the execution a
new timer will be setup with the same time.

**Examples:**

example with [Streams](https://redis.io/docs/manual/data-types/streams/)

1. load [xadd.lua](https://github.com/tzongw/redis-timer/blob/789d78ec7377dee01fd2659eeef70f1dc03dfe5e/xadd.lua) in command line.
```
$ cat xadd.lua | redis-cli -x FUNCTION LOAD REPLACE
"timer"
```
2. do a blocking [XREAD](https://redis.io/commands/xread/) or [XREADGROUP](https://redis.io/commands/xreadgroup/) in redis-cli.
```
127.0.0.1:6379> XREAD block 0 streams jobs 0
```

3. create a timer in another redis-cli.
```
127.0.0.1:6379> TIMER.NEW id timer_xadd 1000 1 jobs field1 value1 field2 value2
```
4. after 1 second, you will see the `XREAD` in step 2 output like this.
```
127.0.0.1:6379> xread block 0 streams jobs 0
1# "jobs" => 1) 1) "1656251442279-0"
      2) 1) "field1"
         2) "value1"
         3) "field2"
         4) "value2"
(1016.89s)
```
5. repeat step 2 using the returning `stream id` (`"1656255594763-0"` in this example).
```
127.0.0.1:6379> xread block 0 streams jobs "1656255594763-0"
```

data types that support blocking read work similarly, like `Lists`, `Sorted Sets`. 

`Pub/Sub` is another option.

**Notes:**

- if a timer with the same name `id` already exists, it will reset the timer.
- when an one-time timer fire, it will be removed from db automatically(though it doesn't have an expiration).
- no info is provided regarding the execution of the script

**Reply:** 0 if reset a timer, 1 if create a new timer.


### `TIMER.KILL id`

Removes a timer.

**Reply:** 1 if `id` exists and is a timer, 0 if `id` does not exist, error if `id` exists but is not a timer.


**Notes:**
- `DEL`(or `SET`) command also serves as removing a timer, but is less efficient. [MORE INFO](https://github.com/tzongw/redis-timer/blob/5a21c598e470df765a4b260a37c3ab4f2bc0e0ed/timer.c#L291)


### `TIMER.INFO id`

Provides info of a timer.

**Reply**: timer info if `id` exists and is a timer, none if `id` does not exist, error if `id` exists but is not a timer.
```
127.0.0.1:6379> TIMER.INFO id
1# "function" => "function"
2# "interval" => (integer) 10000
3# "remaining" => (integer) 6474
4# "loop" => (true)
5# "key1" => "key"
6# "arg1" => "arg"
```

**Notes:**
- `remaining` is milliseconds to the next execution.


## Contributing

Issue reports, pull and feature requests are welcome.


## License

MIT - see [LICENSE](LICENSE)
