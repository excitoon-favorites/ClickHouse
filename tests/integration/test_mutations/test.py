import pytest
import helpers.client
import helpers.cluster


cluster = helpers.cluster.ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
            config_dir='configs',
            main_configs=['configs/logs_config.xml'],
            with_zookeeper=True,
            stay_alive=True,
            macros={"shard": 0, "replica": 1} )

node2 = cluster.add_instance('node2',
            config_dir='configs',
            main_configs=['configs/logs_config.xml'],
            with_zookeeper=True,
            stay_alive=True,
            macros={"shard": 0, "replica": 2} )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_trivial_alter_in_partition_merge_tree_with_where(start_cluster):
    try:
        name = "test_trivial_alter_in_partition_merge_tree_with_where"
        node1.query("CREATE TABLE {} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p".format(name))
        node1.query("INSERT INTO {} VALUES (1, 2), (2, 3)".format(name))
        node1.query("ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        node1.query("ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        node1.query("ALTER TABLE {} DELETE IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["2"]
        node1.query("ALTER TABLE {} DELETE IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["2"]
    finally:
        node1.query("DROP TABLE {}".format(name))


def test_trivial_alter_in_partition_merge_tree_without_where(start_cluster):
    try:
        name = "test_trivial_alter_in_partition_merge_tree_without_where"
        node1.query("CREATE TABLE {} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p".format(name))
        node1.query("INSERT INTO {} VALUES (1, 2), (2, 3)".format(name))
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query("ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 2 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query("ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 3 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query("ALTER TABLE {} DELETE IN PARTITION 2 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query("ALTER TABLE {} DELETE IN PARTITION 3 SETTINGS mutations_sync = 2".format(name))
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
    finally:
        node1.query("DROP TABLE {}".format(name))


def test_trivial_alter_in_partition_replicated_merge_tree(start_cluster):
    try:
        name = "test_trivial_alter_in_partition_replicated_merge_tree"
        node1.query("CREATE TABLE {name} (p Int64, x Int64) ENGINE=ReplicatedMergeTree('/clickhouse/{name}', '1') ORDER BY tuple() PARTITION BY p".format(name=name))
        node2.query("CREATE TABLE {name} (p Int64, x Int64) ENGINE=ReplicatedMergeTree('/clickhouse/{name}', '2') ORDER BY tuple() PARTITION BY p".format(name=name))
        node1.query("INSERT INTO {} VALUES (1, 2)".format(name))
        node2.query("INSERT INTO {} VALUES (2, 3)".format(name))
        node1.query("ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 2 WHERE 1 SETTINGS mutations_sync = 2".format(name))
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        node1.query("ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query("ALTER TABLE {} DELETE IN PARTITION 2 SETTINGS mutations_sync = 2".format(name))
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        node1.query("ALTER TABLE {} DELETE IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["2"]
        node1.query("ALTER TABLE {} DELETE IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(name))
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["2"]
    finally:
        node1.query("DROP TABLE {}".format(name))
        node2.query("DROP TABLE {}".format(name))
