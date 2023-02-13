import pytest
import tempfile

from app import (
    init_spark,
    FindFriends,
    load_graph,
    FriendshipMixin,
)

testcases = {
    "circle-graph": {
        "description": "Circle-graph, extending the originally provided testcase",
        "graph": [("a", "b"), ("b", "c"), ("c", "d"), ("d", "e"), ("e", "a")],
        "cases": [
            (
                1,
                [
                    ("a", ["b", "e"], [1, 1]),
                    ("b", ["a", "c"], [1, 1]),
                    ("c", ["b", "d"], [1, 1]),
                    ("d", ["c", "e"], [1, 1]),
                    ("e", ["a", "d"], [1, 1]),
                ],
            ),
            # For degrees >= 2, we get to the transitive closure covering the entire graph
            (
                2,
                [
                    ("a", ["b", "c", "d", "e"], [1, 2, 2, 1]),
                    ("b", ["a", "c", "d", "e"], [1, 1, 2, 2]),
                    ("c", ["a", "b", "d", "e"], [2, 1, 1, 2]),
                    ("d", ["a", "b", "c", "e"], [2, 2, 1, 1]),
                    ("e", ["a", "b", "c", "d"], [1, 2, 2, 1]),
                ],
            ),
            (
                5,
                [
                    ("a", ["b", "c", "d", "e"], [1, 2, 2, 1]),
                    ("b", ["a", "c", "d", "e"], [1, 1, 2, 2]),
                    ("c", ["a", "b", "d", "e"], [2, 1, 1, 2]),
                    ("d", ["a", "b", "c", "e"], [2, 2, 1, 1]),
                    ("e", ["a", "b", "c", "d"], [1, 2, 2, 1]),
                ],
            ),
        ],
    },
    "example-graph": {
        "description": "Example graph, extending the originally provided testcase",
        "graph": [
            ("david", "paula"),
            ("david", "kim"),
            ("kim", "tim"),
            ("tim", "paula"),
            ("ann", "tim"),
            ("watson", "david"),
            ("mary", "watson"),
        ],
        "cases": [
            (
                1,
                [
                    ("ann", ["tim"], [1]),
                    ("david", ["kim", "paula", "watson"], [1, 1, 1]),
                    ("kim", ["david", "tim"], [1, 1]),
                    ("mary", ["watson"], [1]),
                    ("paula", ["david", "tim"], [1, 1]),
                    ("tim", ["ann", "kim", "paula"], [1, 1, 1]),
                    ("watson", ["david", "mary"], [1, 1]),
                ],
            ),
            (
                2,
                [
                    ("ann", ["kim", "paula", "tim"], [2, 2, 1]),
                    ("david", ["kim", "mary", "paula", "tim", "watson"], [1, 2, 1, 2, 1]),
                    ("kim", ["ann", "david", "paula", "tim", "watson"], [2, 1, 2, 1, 2]),
                    ("mary", ["david", "watson"], [2, 1]),
                    ("paula", ["ann", "david", "kim", "tim", "watson"],[2, 1, 2, 1, 2]),
                    ("tim",["ann", "david", "kim", "paula"],[1, 2, 1, 1]),
                    ("watson",["david", "kim", "mary", "paula"],[1, 2, 1, 2],),
                ],
            ),
            (
                3,
                [
                    ("ann",["david", "kim", "paula", "tim"],[3, 2, 2, 1]),
                    ("david",["ann", "kim", "mary", "paula", "tim", "watson"],[3, 1, 2, 1, 2, 1]),
                    ("kim",["ann","david","mary","paula","tim","watson",],[2, 1, 3, 2, 1, 2]),
                    ("mary",["david", "kim", "paula", "watson"],[2, 3, 3, 1]),
                    ("paula",["ann","david","kim","mary","tim","watson",],[2, 1, 2, 3, 1, 2]),
                    ("tim",["ann", "david", "kim", "paula", "watson"],[1, 2, 1, 1, 3]),
                    ("watson",["david", "kim", "mary", "paula", "tim"],[1, 2, 1, 2, 3]),
                ],
            ),
        ],
    },
    "circle-and-dead-end": {
        "description": "Circle-graph, extending the originally provided testcase",
        "graph": [
            ("a", "b"),
            ("b", "c"),
            ("c", "d"),
            ("d", "e"),
            ("e", "a"),
            ("e", "f"),
            ("f", "g"),
            ("g", "h"),
            ("h", "i"),
            ("i", "j"),
            ("j", "k"),
        ],
        "cases": [
            (
                5,
                [
                    ("a",["b", "c", "d", "e", "f", "g", "h", "i"],[1, 2, 2, 1, 2, 3, 4, 5],),
                    ("b",["a", "c", "d", "e", "f", "g", "h"],[1, 1, 2, 2, 3, 4, 5],),
                    ("c",["a", "b", "d", "e", "f", "g", "h"],[2, 1, 1, 2, 3, 4, 5],),
                    ("d",["a", "b", "c", "e", "f", "g", "h", "i"],[2, 2, 1, 1, 2, 3, 4, 5],),
                    ("e",["a", "b", "c", "d", "f", "g", "h", "i", "j"],[1, 2, 2, 1, 1, 2, 3, 4, 5],),
                    ("f",["a", "b", "c", "d", "e", "g", "h", "i", "j", "k"],[2, 3, 3, 2, 1, 1, 2, 3, 4, 5],),
                    ("g",["a", "b", "c", "d", "e", "f", "h", "i", "j", "k"],[3, 4, 4, 3, 2, 1, 1, 2, 3, 4],),
                    ("h",["a", "b", "c", "d", "e", "f", "g", "i", "j", "k"],[4, 5, 5, 4, 3, 2, 1, 1, 2, 3],),
                    ("i",["a", "d", "e", "f", "g", "h", "j", "k"],[5, 5, 4, 3, 2, 1, 1, 2],),
                    ("j", ["e", "f", "g", "h", "i", "k"], [5, 4, 3, 2, 1, 1]),
                    ("k", ["f", "g", "h", "i", "j"], [5, 4, 3, 2, 1]),
                ],
            ),
        ],
    },
    "two-islands": {
        "description": "Two disconnected subgraphs.",
        "graph": [
            ("a", "b"),
            ("b", "c"),
            ("d", "c"),
            ("d", "e"),
            ("f", "g"),
            ("g", "h"),
            ("h", "f"),
        ],
        "cases": [
            (
                2,
                [
                    ("a", ["b", "c"], [1, 2]),
                    ("b", ["a", "c", "d"], [1, 1, 2]),
                    ("c", ["a", "b", "d", "e"], [2, 1, 1, 2]),
                    ("d", ["b", "c", "e"], [2, 1, 1]),
                    ("e", ["c", "d"], [2, 1]),
                    ("f", ["g", "h"], [1, 1]),
                    ("g", ["f", "h"], [1, 1]),
                    ("h", ["f", "g"], [1, 1]),
                ],
            ),
        ],
    },
}


@pytest.fixture(scope="session")
def spark_session(request):
    tmp = tempfile.TemporaryDirectory()
    print(tmp.name)
    request.addfinalizer(lambda: tmp.cleanup())
    spark = init_spark(tmp.name)
    request.addfinalizer(lambda: spark.stop())

    return spark


@pytest.fixture()
def ff_object(request):
    return FindFriends()


pytest_mark = pytest.mark.usefixtures("spark_session")


def pytest_generate_tests(metafunc):
    """
    Generates parameterization scheme based on the 'test_cases' global variable.

    Note that this function will get called once for each test function in the module.
    However, we would like to execute the enclosed block of code only in conjunction with `test_graph()`.
    To achieve that, we are checking if there is a match with the signature of `test_graph()`.
    """
    if {"graph", "degree", "expected"}.issubset(set(metafunc.fixturenames)):

        def test_id(name, degree):
            return f"{name}, degree={degree}"

        ids, argvalues = zip(
            *[
                (
                    test_id(name, degree),
                    (test_bundle["graph"], degree, expected),
                )
                for name, test_bundle in testcases.items()
                for degree, expected in test_bundle["cases"]
            ]
        )
        metafunc.parametrize(["graph", "degree", "expected"], argvalues, ids=ids)


def test_find_friends(spark_session, ff_object, graph, degree, expected):
    """Driver for the graph algo"""
    in_df = spark_session.sparkContext.parallelize(graph, 1).toDF(["src", "dst"])
    results = ff_object.find_friends(*load_graph(in_df), degree)
    friendships = (
        results.select("id", "friends", "distances")
        .rdd.map(lambda x: (x[0], x[1], x[2]))
        .collect()
    )
    assert friendships == expected


@pytest.mark.parametrize(
    "shortest, other, expected",
    [
        ({}, [], ({}, {})),
        ({}, [{}], ({}, {})),
        ({}, [{}, {}], ({}, {})),
        ({"a": "b"}, [{}], ({"a": "b"}, {})),
        (
            {"a": "b"},
            [{"c": "d"}, {"a": "f"}],
            ({"a": "b", "c": "d"}, {"c": "d"}),
        ),
        (
            {"a": "f"},
            [{"c": "d"}, {"a": "b"}],
            ({"a": "b", "c": "d"}, {"a": "b", "c": "d"}),
        ),
        (
            {"a": 1, "d": 1, "e": 0},
            [{"e": 2, "b": 2}, {"e": 2, "c": 2}],
            ({"a": 1, "b": 2, "c": 2, "d": 1, "e": 0}, {"b": 2, "c": 2}),
        ),
    ],
)
def test_update_shortest_distance(shortest, other, expected):
    assert FriendshipMixin.update_shortest_distance(shortest, other) == expected


@pytest.mark.parametrize(
    "test_input, expected_vertices, expected_edges",
    [
        ([("a", "b"), ("b", "c")], ["a", "b", "c"], [["a", "b"], ["b", "c"]]),
        ([("b", "a"), ("c", "b")], ["a", "b", "c"], [["b", "a"], ["c", "b"]]),
    ],
)
def test_graph_is_loaded(spark_session, test_input, expected_vertices, expected_edges):
    in_df = spark_session.sparkContext.parallelize(test_input, 1).toDF(["src", "dst"])
    vertices, edges = load_graph(in_df)
    assert sorted(vertices.rdd.map(lambda x: x[0]).collect()) == expected_vertices
    assert sorted(edges.rdd.map(lambda x: [x[0], x[1]]).collect()) == expected_edges
