from distcache.hashring import HashRing


def test_empty_ring_returns_none():
    ring = HashRing()
    assert ring.get_node("anything") is None
    assert ring.get_preference_list("anything", 3) == []


def test_single_node_owns_everything():
    ring = HashRing(["a"])
    for i in range(100):
        assert ring.get_node(f"key{i}") == "a"


def test_lookup_is_deterministic():
    ring = HashRing(["a", "b", "c"])
    first = [ring.get_node(f"key{i}") for i in range(200)]
    second = [ring.get_node(f"key{i}") for i in range(200)]
    assert first == second


def test_removing_a_node_only_moves_that_nodes_keys():
    ring = HashRing(["a", "b", "c", "d", "e"], vnodes=150)
    keys = [f"key{i}" for i in range(2000)]
    before = {key: ring.get_node(key) for key in keys}

    ring.remove_node("c")

    moved = 0
    for key, old_owner in before.items():
        new_owner = ring.get_node(key)
        assert new_owner != "c"
        if new_owner != old_owner:
            moved += 1
            assert old_owner == "c"

    assert moved == sum(1 for owner in before.values() if owner == "c")


def test_adding_a_node_only_steals_a_fraction_of_keys():
    ring = HashRing(["a", "b", "c", "d"], vnodes=150)
    keys = [f"key{i}" for i in range(2000)]
    before = {key: ring.get_node(key) for key in keys}

    ring.add_node("e")

    moved = sum(1 for key, owner in before.items() if ring.get_node(key) != owner)
    fraction = moved / len(keys)

    # roughly 1/5, with generous bounds
    assert 0.05 < fraction < 0.45


def test_preference_list_has_distinct_physical_nodes():
    ring = HashRing(["a", "b", "c", "d", "e"], vnodes=150)
    for i in range(200):
        prefs = ring.get_preference_list(f"key{i}", 3)
        assert len(prefs) == 3
        assert len(set(prefs)) == 3


def test_preference_list_capped_by_node_count():
    ring = HashRing(["a", "b"], vnodes=150)
    prefs = ring.get_preference_list("key", 5)
    assert len(prefs) == 2
    assert set(prefs) == {"a", "b"}


def test_preference_list_starts_at_same_node_as_get_node():
    ring = HashRing(["a", "b", "c", "d"], vnodes=150)
    for i in range(200):
        key = f"key{i}"
        assert ring.get_preference_list(key, 3)[0] == ring.get_node(key)


def test_readding_a_removed_node_restores_its_ownership():
    ring = HashRing(["a", "b", "c"], vnodes=150)
    keys = [f"key{i}" for i in range(500)]
    before = {key: ring.get_node(key) for key in keys}

    ring.remove_node("b")
    ring.add_node("b")

    after = {key: ring.get_node(key) for key in keys}
    assert before == after


def test_add_node_twice_is_a_no_op():
    ring = HashRing(["a"], vnodes=150)
    points_before = len(ring._sorted_points)
    ring.add_node("a")
    assert len(ring._sorted_points) == points_before


def test_key_distribution_is_reasonably_even_with_vnodes():
    ring = HashRing(["a", "b", "c", "d", "e"], vnodes=150)
    counts = {node: 0 for node in ring.nodes}
    for i in range(20000):
        counts[ring.get_node(f"randkey{i}")] += 1

    avg = 20000 / 5
    for count in counts.values():
        assert 0.7 * avg < count < 1.3 * avg
