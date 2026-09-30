# Notification-based result waiting: local race proofs

`demo_result_notifications.py` proves that clients waiting on job results can
never hang when results arrive by notification. It deliberately arranges three
race timing points and checks expiry-vs-not-found plus connection recovery:

1. **completion in the subscribe window** - the result is persisted (and
   pinged) strictly between the waiter's pre-subscribe state check and its
   SUBSCRIBE being confirmed;
2. **notification lost in a connection drop** - the pub/sub connection is
   severed and the job finishes during the outage, so the PUBLISH reaches
   nobody; the reader rebuilds, wakes everyone to re-check state and the
   persisted result is returned;
3. **shared subscription with one cancellation** - four waiters share a single
   subscription, one is cancelled mid-wait, and the other three still wake on
   the single PUBLISH.

Run:

```
.venv/bin/python scripts/demo_result_notifications.py
```

If a real redis answers on `localhost:6379` it is used automatically; pass
`--emulator` to force the bundled stand-in. This checkout was built in a
sandbox that forbids all sockets (even loopback) and ships no redis binary, so
the default here is `redis_emu.py`: a small, in-process, **pipe-backed RESP2**
server. It speaks real RESP over `os.pipe` pairs, so the unmodified redis-py
client (pipelines, transactions and pub/sub) drives it - only the OS transport
is faked. `drop_connection()` emulates a server/network outage by closing the
server end of the pipes.
