# Timer Redis Module

This module allows the delayed execution of LUA scripts, both periodic and one-time.

# Features

0. High performance.
0. Supports persistence, both RDB and AOF.
0. Supports replication and cluster.

## Quick start

1. Obtain a redis server version 7.0 or above (with [function](https://redis.io/docs/manual/programmability/functions-intro/) support)
2. Download and compile redis-timer (`make`)
3. Start the redis server with the timer module:
    - By adding this line to redis.conf: `loadmodule /path/to/timer.so`
    - By using a command-line argument: `redis-server --load-module /path/to/timer.so`
    - By running the command: `MODULE LOAD /path/to/timer.so`


## Commands

### `TIMER.NEW id function milliseconds [LOOP] numkeys [key [key ...]] [arg [arg ...]]`

Create and activate a new timer. Also an value with name `id` will be created in redis db.
```
127.0.0.1:6379> TIMER.NEW id function 10000 LOOP 1 key arg
(integer) 1
127.0.0.1:6379> type id
timer-tzw
```

The `function` will be
executed after `milliseconds` with `numkeys [key [key ...]] [arg [arg ...]]` as arguments via [FCALL](https://redis.io/commands/fcall/). If `LOOP` is specified, after the execution a
new timer will be setup with the same time.

**Notes:**

- no info is provided regarding the execution of the script
- for simplicity, in periodic timers the execution interval will start counting at the end of the previous execution, and not at the beginning. After some time, the exact time of the triggering may be difficult to deduce, particularly if the the script takes a long time to execute or if different executions require different ammounts of time.
- if a timer with the same name `id` already exists, it will reset the timer.

**Reply:** 0 if reset a timer, 1 if create a new timer.


### `TIMER.KILL id`

Removes a timer.

**Reply:** 1 if `id` exists and is a timer, 0 if `id` does not exist, error if `id` exists but is not a timer.


**Notes:**
- `DEL` command also works as removing a timer, but it's less efficient. [MORE INFO](https://github.com/tzongw/redis-timer/blob/5a21c598e470df765a4b260a37c3ab4f2bc0e0ed/timer.c#L291)


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
