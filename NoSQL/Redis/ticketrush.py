"""TicketRush Redis lab, Steps 1–6."""
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import redis

r = redis.Redis(
    host="127.0.0.1",
    port=int(os.environ.get("REDIS_PORT", "6379")),
    socket_connect_timeout=3,
    decode_responses=True,
)


def race(fn, n=50):
    start = Event()

    def worker(index):
        start.wait()
        return fn(index)

    with ThreadPoolExecutor(max_workers=n) as pool:
        try:
            pending = [pool.submit(worker, index) for index in range(n)]
        finally:
            start.set()
        return [job.result() for job in pending]


def hold_naive(event, seat, user):
    key = f"hold:{event}:{seat}"

    if r.get(key) is None:
        time.sleep(0.01)
        r.set(key, user, ex=300)
        return True

    return False


def hold_nx(event, seat, user, ttl=300):
    key = f"hold:{event}:{seat}"
    return bool(r.set(key, user, nx=True, ex=ttl))

HOLD = r.register_script("""
if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then return 0 end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3]) then return 1 end
return 0
""")

CONFIRM = r.register_script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SADD', KEYS[2], ARGV[2])
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
""")

RELEASE = r.register_script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
""")


def hold(event, seat, user, ttl=300):
    return bool(HOLD(keys=[f"hold:{event}:{seat}", f"sold:{event}"], args=[user, seat, ttl]))


def confirm(event, seat, user):
    return bool(CONFIRM(keys=[f"hold:{event}:{seat}", f"sold:{event}"], args=[user, seat]))


def release(event, seat, user):
    return bool(RELEASE(keys=[f"hold:{event}:{seat}"], args=[user]))


def seat_map(event, seats):
    sold = r.smembers(f"sold:{event}")
    return {
        s: "sold" if s in sold else "held" if r.exists(f"hold:{event}:{s}") else "free"
        for s in seats
    }

# DECR nguyên tử: trừ và trả về số lượng mới
def buy_general(event, user):
    key = f"stock:{event}"
    left = r.decr(key)

    if left < 0:
        r.incr(key)  # Hoàn lại vì hết vé
        return False

    r.sadd(f"buyers:{event}", user)
    return True


# Cố tình viết sai: kiểm tra rồi mới trừ
def buy_general_naive(event, user):
    key = f"stock:{event}"

    if int(r.get(key)) > 0:
        time.sleep(0.01)  # Làm lỗi tranh chấp dễ xuất hiện
        r.decr(key)
        r.sadd(f"buyers:{event}", user)
        return True

    return False

def allow_fixed(user, limit=5, window=10, *, now=None):
    now = time.time() if now is None else now
    key = f"rl:fixed:{user}:{int(now // window)}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window, nx=True)    # set the TTL only the first time
    count, _ = pipe.execute()
    return count <= limit

SLIDING = r.register_script("""
local now, window, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
if redis.call('ZCARD', KEYS[1]) < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  redis.call('PEXPIRE', KEYS[1], window)
  return 1
end
return 0
""")


def allow_sliding(user, limit=5, window_ms=10_000, *, now_ms=None):
    now = int(time.time() * 1000) if now_ms is None else now_ms
    return bool(SLIDING(keys=[f"rl:sliding:{user}"], args=[now, window_ms, limit, uuid.uuid4().hex]))

def try_hold(event, seat, user):
    if not allow_sliding(user):
        return "rate_limited"
    return "ok" if hold(event, seat, user) else "taken"

def join_queue(event, user):
    r.zadd(f"queue:{event}", {user: time.time()}, nx=True)   # NX: re-joining keeps your place
    return r.zrank(f"queue:{event}", user) + 1


def admit(event, n):
    return [user for user, _ in r.zpopmin(f"queue:{event}", n)]

SEATS = [f"S{i}" for i in range(1, 21)]


def check(label, actual, expected):
    print(f"{label}: {actual}")
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def demo_holds(run):
    print("\nStep 1 | Seat-hold race")
    for attempt in range(3):
        event = f"{run}-naive-{attempt}"
        winners = race(lambda i: hold_naive(event, "A1", f"user{i}"))
        print(f"GET + SET, trial {attempt + 1}: {sum(winners)} winners")

    event = f"{run}-nx"
    winners = race(lambda i: hold_nx(event, "A1", f"user{i}"))
    check("SET NX EX winners", sum(winners), 1)
    ttl = r.ttl(f"hold:{event}:A1")
    print(f"Hold TTL: {ttl} seconds")
    check("TTL is set", 0 < ttl <= 300, True)


def demo_lua(run):
    print("\nStep 2 | Atomic hold, confirm and release")
    event = f"{run}-lua"
    check("Linh holds A1", hold(event, "A1", "linh"), True)
    check("Nam cannot hold A1", hold(event, "A1", "nam"), False)
    check("Nam cannot release A1", release(event, "A1", "nam"), False)
    check("Nam cannot confirm A1", confirm(event, "A1", "nam"), False)
    check("Linh confirms A1", confirm(event, "A1", "linh"), True)
    check("Sold seats", r.smembers(f"sold:{event}"), {"A1"})
    check("Sold A1 cannot be held again", hold(event, "A1", "nam"), False)
    check("Linh holds A2", hold(event, "A2", "linh"), True)
    check("Linh releases A2", release(event, "A2", "linh"), True)
    check("Seat map", seat_map(event, ["A1", "A2"]), {"A1": "sold", "A2": "free"})


def demo_stock(run):
    print("\nStep 3 | 500 requests for 100 standing tickets")
    for label, buy in (("DECR", buy_general), ("GET then DECR", buy_general_naive)):
        event = f"{run}-{label.split()[0]}"
        r.set(f"stock:{event}", 100)
        winners = race(lambda i: buy(event, f"user{i}"), n=500)
        result = (sum(winners), int(r.get(f"stock:{event}")), r.scard(f"buyers:{event}"))
        if buy is buy_general:
            check(f"{label} (sales, stock left, buyers)", result, (100, 0, 100))
        else:
            print(f"{label} (sales, stock left, buyers): {result}")


def demo_rate_limits(run):
    print("\nStep 4 | Rate limiting")
    check("Fixed window: eight attempts", [allow_fixed(f"{run}-fixed") for _ in range(8)],
          [True] * 5 + [False] * 3)

    # Simulate arrivals 200ms before and after a ten-second boundary.
    boundary = (int(time.time()) // 10 + 1) * 10
    before, after = boundary - 0.2, boundary + 0.2
    fixed = [allow_fixed(f"{run}-fixed-bot", now=when)
             for when in (before, after) for _ in range(5)]
    sliding = [allow_sliding(f"{run}-sliding-bot", now_ms=int(when * 1000))
               for when in (before, after) for _ in range(5)]
    check("Fixed window: boundary burst", fixed, [True] * 10)
    check("Sliding window: boundary burst", sliding, [True] * 5 + [False] * 5)

    statuses = race(lambda i: try_hold(f"{run}-limited", f"S{i}", f"{run}-bot"))
    check("Concurrent bot requests (allowed, blocked)",
          (statuses.count("ok"), statuses.count("rate_limited")), (5, 45))


def demo_queue(run):
    print("\nStep 5 | Waiting room")
    check("Initial positions", [join_queue(run, user) for user in ("u1", "u2", "u3")],
          [1, 2, 3])
    check("u1 rejoins", join_queue(run, "u1"), 1)
    check("First two admitted", admit(run, 2), ["u1", "u2"])
    check("u3 moves to first", join_queue(run, "u3"), 1)


def demo_stress(run):
    print("\nStep 6 | 100 users competing for 20 seats, ten trials")
    for trial in range(1, 11):
        event = f"{run}-stress-{trial}"

        def buyer(index):
            user = f"user{index}"
            for seat in SEATS:
                if hold(event, seat, user):
                    time.sleep(random.random() * 0.05)
                    if confirm(event, seat, user):
                        return seat
            return None

        seats = [seat for seat in race(buyer, n=100) if seat is not None]
        check(f"Trial {trial} (sales, distinct, Redis sold set)",
              (len(seats), len(set(seats)), r.scard(f"sold:{event}")), (20, 20, 20))


def main():
    try:
        r.ping()
    except redis.exceptions.RedisError as error:
        raise SystemExit(f"Redis connection failed. Start it with docker compose up -d: {error}")

    run = f"ticketrush-{uuid.uuid4().hex}"
    try:
        demo_holds(run)
        demo_lua(run)
        demo_stock(run)
        demo_rate_limits(run)
        demo_queue(run)
        demo_stress(run)
        print("\nPASS: TicketRush lab checks completed.")
    finally:
        # Remove only keys created by this run.
        patterns = (f"hold:{run}-*", f"sold:{run}-*", f"stock:{run}-*",
                    f"buyers:{run}-*", f"rl:fixed:{run}-*", f"rl:sliding:{run}-*",
                    f"queue:{run}")
        for pattern in patterns:
            keys = list(r.scan_iter(match=pattern))
            if keys:
                r.delete(*keys)


if __name__ == "__main__":
    main()
