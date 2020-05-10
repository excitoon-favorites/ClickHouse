from contextlib import contextmanager

import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance_test_mutations = cluster.add_instance('test_mutations_with_merge_tree', main_configs=['configs/config.xml'], user_configs=['configs/users.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance_test_mutations.query('''CREATE TABLE test_mutations_with_ast_elements(date Date, a UInt64, b String) ENGINE = MergeTree(date, (a, date), 8192)''')
        instance_test_mutations.query('''INSERT INTO test_mutations_with_ast_elements SELECT '2019-07-29' AS date, 1, toString(number) FROM numbers(1)''')
        yield cluster
    finally:
        cluster.shutdown()


def test_mutations_with_merge_background_task(started_cluster):
    instance_test_mutations.query('''SYSTEM STOP MERGES test_mutations_with_ast_elements''')

    ## The number of asts per query is 15
    for execution_times_for_mutation in range(100):
        instance_test_mutations.query('''ALTER TABLE test_mutations_with_ast_elements DELETE WHERE 1 = 1 AND toUInt32(b) IN (1)''')

    all_done = False
    for wait_times_for_mutation in range(100): # wait for replication 80 seconds max
        time.sleep(0.8)

        def get_done_mutations(instance):
            instance_test_mutations.query('''DETACH TABLE test_mutations_with_ast_elements''')
            instance_test_mutations.query('''ATTACH TABLE test_mutations_with_ast_elements''')
            return int(instance.query("SELECT sum(is_done) FROM system.mutations WHERE table = 'test_mutations_with_ast_elements'").rstrip())

        if get_done_mutations(instance_test_mutations) == 100:
            all_done = True
            break

    print instance_test_mutations.query("SELECT mutation_id, command, parts_to_do, is_done FROM system.mutations WHERE table = 'test_mutations_with_ast_elements' FORMAT TSVWithNames")
    assert all_done

def test_mutations_with_truncate_table(started_cluster):
    instance_test_mutations.query('''SYSTEM STOP MERGES test_mutations_with_ast_elements''')

    ## The number of asts per query is 15
    for execute_number in range(100):
        instance_test_mutations.query('''ALTER TABLE test_mutations_with_ast_elements DELETE WHERE 1 = 1 AND toUInt32(b) IN (1)''')

    instance_test_mutations.query("TRUNCATE TABLE test_mutations_with_ast_elements")
    assert instance_test_mutations.query("SELECT COUNT() FROM system.mutations WHERE table = 'test_mutations_with_ast_elements'").rstrip() == '0'


def test_lots_of_mutations(started_cluster):
    name = 'test_lots_of_mutations'

    instance_test_mutations.query('''CREATE TABLE {name} (p UInt64, b UInt64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY p'''.format(name=name))

    try:
        parts = 45
        partitions_per_insert = 100
        queries = []
        for i in range(parts):
            if i % partitions_per_insert == 0:
                queries.append(['''INSERT INTO {name} VALUES'''.format(name=name)])
            queries[-1].append('({i}, 1)'.format(i=i))
        jobs = []
        for query in queries:
            jobs.append(threading.Thread(target=instance_test_mutations.query, args=(' '.join(query),)))

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()

        def spam_parts(t):
            while time.time() < t:
                instance_test_mutations.query('''SELECT * FROM system.parts WHERE name = '{name}' '''.format(name=name))

        start_time = time.time()
        finish = start_time + 150

        jobs = []
        for n in range(50):
        # 50, 10 -- 95m
        # 50, 25 -- 30m
        # 75, 50 -- 30m
        # 50, 50 -- 23m
        # 45, 50 -- 6m, 16m
        # 35, 50 -- 91m
        # 30, 50 -- 79m
        # 25, 50 -- 13m
        # 20, 50 -- 37m
        # 10, 50 -- 95m
        # 45, 50, 28631f461ed85b5d9d255fd96381a8bd20ea47f2 -- 431m
            jobs.append(threading.Thread(target=spam_parts, args=(finish,)))

        for job in jobs:
            job.start()

        attempt = 0
        while time.time() < finish:
            attempt += 1
            instance_test_mutations.query('''ALTER TABLE {name} UPDATE b=b WHERE p=1 OR 'attempt {attempt}' == 'time {time}' SETTINGS mutations_sync=1'''.format(attempt=attempt, time=time.time()-start_time, name=name), timeout=60)

        for job in jobs:
            job.join()

    finally:
        try:
            instance_test_mutations.query('''DROP TABLE {name}'''.format(name=name))
        except:
            pass
