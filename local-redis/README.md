# Local Redis for tests / demos

The serial-group feature is designed for a real Redis. This directory contains
tooling to exercise it locally, including on machines where installing or
running `redis-server` is not possible.

## Files

- `mini_redis.py` — a small, dependency-free server speaking the RESP protocol
  over TCP. Run it as an **independent local Redis process**:
  ```bash
  python local-redis/mini_redis.py --host 127.0.0.1 --port 6399
  ```
  It implements only the commands arq uses (strings, sorted sets, lists,
  transactions with WATCH/MULTI/EXEC, key expiry).
- `inproc_redis.py` — the same command engine wired straight into redis-py,
  for sandboxes that block the `socket()` syscall. No network, no subprocess.
- `verify_serial_groups.py` — end-to-end scenario checks (see below).

`tests/conftest.py` selects the in-process engine automatically when
`ARQ_TEST_NO_DOCKER=1` is set, so no plugin is needed.

## Running against a real independent Redis

```bash
redis-server --port 6399 --daemonize yes
ARQ_TEST_REDIS_HOST=127.0.0.1 ARQ_TEST_REDIS_PORT=6399 pytest
ARQ_TEST_REDIS_HOST=127.0.0.1 ARQ_TEST_REDIS_PORT=6399 \
    python local-redis/verify_serial_groups.py
```

Or using the mini server instead of redis-server:

```bash
python local-redis/mini_redis.py --port 6399 &
ARQ_TEST_REDIS_HOST=127.0.0.1 ARQ_TEST_REDIS_PORT=6399 pytest
```

## Running with no sockets at all

```bash
ARQ_TEST_NO_DOCKER=1 pytest
ARQ_TEST_NO_DOCKER=1 python local-redis/verify_serial_groups.py
```

## Scenarios verified

1. two workers share one Redis: different groups run in parallel, members of a
   group run strictly one at a time in enqueue order;
2. a failing job retries at the head of the group — the sibling only starts
   once the retry succeeds;
3. a runner that vanishes while holding the group cannot wedge it: the group
   lock (and in-progress marker) expire and another worker re-runs the head;
4. a deferred head blocks the group until it is due, then FIFO resumes.
