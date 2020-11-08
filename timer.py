# -*- coding: utf-8 -*-
from redis import Redis
from redis.client import bool_ok


class Timer:
    def __init__(self, redis: Redis):
        self._redis = redis
        self._script2sha = {}

    def timer_new(self, key: str, data: str, sha: str, interval: int, loop=False):
        params = [key, data, sha, interval]
        if loop:
            params.append('LOOP')
        res = self._redis.execute_command('TIMER.NEW', *params)
        return bool_ok(res)

    def timer_kill(self, *keys):
        res = self._redis.execute_command('TIMER.KILL', *keys)
        return int(res)

    def stream_timer_new(self, key: str, data: str, interval: int, stream: str, maxlen=4096, loop=False):
        script = f"return redis.call('XADD', '{stream}', 'MAXLEN', '~', '{maxlen}', '*', '', ARGV[1])"
        sha = self._script2sha.get(script)
        if sha is None:
            sha = self._redis.script_load(script)
            self._script2sha[script] = sha
        return self.timer_new(key, data, sha, interval, loop)


