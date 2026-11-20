from distcache.lru import LRUCache


def test_get_missing_returns_none():
    cache = LRUCache(capacity=2)
    assert cache.get("x") is None


def test_put_then_get():
    cache = LRUCache(capacity=2)
    cache.put("a", 1)
    assert cache.get("a") == 1


def test_evicts_least_recently_used_when_full():
    cache = LRUCache(capacity=2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)  # evicts "a", the least recently used
    assert cache.get("a") is None
    assert cache.get("b") == 2
    assert cache.get("c") == 3


def test_get_counts_as_recent_use():
    cache = LRUCache(capacity=2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.get("a")       # "a" is now more recent than "b"
    cache.put("c", 3)    # evicts "b", not "a"
    assert cache.get("b") is None
    assert cache.get("a") == 1
    assert cache.get("c") == 3


def test_overwrite_updates_value_and_recency():
    cache = LRUCache(capacity=2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("a", 99)   # touches "a", makes "b" the least recent
    cache.put("c", 3)    # evicts "b"
    assert cache.get("b") is None
    assert cache.get("a") == 99


def test_delete_removes_key():
    cache = LRUCache(capacity=2)
    cache.put("a", 1)
    cache.delete("a")
    assert cache.get("a") is None
    assert len(cache) == 0


def test_delete_missing_key_is_a_noop():
    cache = LRUCache(capacity=2)
    cache.delete("nope")  # must not raise
    assert len(cache) == 0
