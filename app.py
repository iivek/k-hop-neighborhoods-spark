import string
import tempfile
from typing import Tuple, Iterable, Dict

import findspark
from argparse import ArgumentParser

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    MapType,
    StringType,
    ArrayType,
    ShortType,
    StructType,
    StructField,
)


def init_spark(tmp_dir) -> SparkSession:
    findspark.init()
    session = (
        SparkSession.builder.config(
            "spark.jars.packages",
            "graphframes:graphframes:0.8.1-spark3.0-s_2.12",
        )
        .appName("k-hop-neighborhood")
        .getOrCreate()
    )
    session.sparkContext.setCheckpointDir(tmp_dir)
    return session


class FriendshipMixin:
    """Home to the user-defined functions.

    Attributes:
        sd_schema: DataFrame schema that represents shortest distance to a vertex.
    """

    sd_schema = MapType(StringType(), ShortType())

    @staticmethod
    def update_shortest_distance(
        shortest: Dict, candidates: Iterable[Dict]
    ) -> Tuple[Dict, Dict]:
        """
        Assuming `shortest` holds shortest distances from one vertex to a set of vertices are stored in a dictionary,
        the function updates `shortest` with entries from `other`.

        Args:
            shortest: Organised as {`vertex_id_A`: distance_A, ... `vertex_id_I`: distance_I`}
            candidates: Organised as {`vertex_id_A`: distance_A, ... `vertex_id_I`: distance_I`}

        Returns:
            Dictionary of shortest distances and a subset of `candidates`that made it into `shortest`

        """
        newly_added = {}
        for elem in candidates:
            for k, v in elem.items():
                if shortest.get(k) is None or v <= shortest.get(k):
                    shortest[k] = v
                    newly_added.update({k: shortest[k]})
        return shortest, newly_added

    def __init__(self):
        self.bump_udf = F.udf(
            lambda x: {k: v + 1 for k, v in x.updated.items()}
            if x.updated
            else None,
            self.sd_schema,
        )
        """Distance is stored relative to the vertex's 1-hop neighbors"""

        self.update_shortest_path_udf = F.udf(
            FriendshipMixin.update_shortest_distance,
            StructType(
                [
                    StructField("friends", self.sd_schema, False),
                    StructField("updated", self.sd_schema, False),
                ]
            ),
        )
        """Maintains shortest paths between node pairs when new pairs are added merge_columns"""

        self.merge_columns_udf = F.udf(
            lambda *x: sum([*x], []), ArrayType(self.sd_schema)
        )


class FindFriends(FriendshipMixin):
    """Finds nodes reachable from a node within k-hops, for all nodes. Horizontally scalable, using GraphFrames.

    Remains scalable in case of large choices of k, or hops, because of the following design choices:
    1) keeps message sizes under control by removing redundant vertices
    2) as soon as the entire search space has been explored for a vertex, message propagation from that vertex is stopped

    Because we're using short integers (see `FriendshipMixin.sd_schema`),`degree` is effectively limited to 32767.
    """

    def initialize_bfs(
        self, vertices: DataFrame, edges: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """Prepares dataframes for the message passing algorithm. Next to representing the graph as vertices and edges,
        Specifically, for each vertex, neighborhoods are initialized by adding a self-reference with distance of 0.

        Args:
            vertices:
            edges:

        Returns:

        """
        distance_to_self = F.array(F.create_map(vertices.id, F.lit(0)))
        friends = vertices.withColumn(
            "bfs",
            self.update_shortest_path_udf(F.create_map(), distance_to_self),
        )

        return friends, edges

    def bfs(self, vertices, edges, degree: int) -> DataFrame:
        """A batch synchronous implementation of breadth-first search based on AggregateMessages primitive.

        K-hop neighborhoods are represented as vertex-to-degree maps (dictionaries), e.g. as {e -> 0, a -> 1, d -> 1}.
        For each vertex, the algorithm iteratively expands the search by one hop.

        Algorithm steps
        0. Neighborhoods get initialized for each vertex by adding self to the set.
        1. Send newly added neighbors as outgoing messages
        2. Aggregate received messsage
        3. Join nodes from the received message with neighborhood. Isolate and update newly added neighbors.
        4. Repeat 1.

        To keep message passing between vertices lightweight, the algorithm only propagates those vertices that are a
        new addition to the k-hop neighborhood; repeatedly sending from previous traversals would be redundant and cause
        scalability issues.
        Another performance-related tweak is a mechanism that recognizes when breadth-first search can be terminated.
        If an iteration of breadth-first search hasn't returned any new friends, this means we've traversed all the
        connected nodes and can call it off; the search would otherwise be stuck in a loop or retracing a cul-de-sac,
        or possibly both.

        Important:
        By convention, the function treats every edge as bidirectional, sending messages both to the inbound and
        outbound edges. The assumption here is that duplicate pairs don't exist, and an internal mechanism that
        checks for duplicates at runtime does not exist
        So, for example, take care not to use both "david" -> "tim" and "tim" -> "david"
        in edge definitions, otherwise you risk slowing down the computation.

        Potential algorithmic improvements:
        A single vertex currently broadcasts the same message to all the outbound edges indiscriminantly. By not
        returning the received message back to the sender down the same edge, messages can be made even shorter.

        Args:
            vertices:
            edges:
            degree:

        Returns:
            +---+------------------------+
            |id |bfs                     |
            +---+------------------------+
            |e  |{a -> 1, d -> 1, e -> 0}|
            |d  |{c -> 1, d -> 0, e -> 1}|
        """
        friends, edges = self.initialize_bfs(vertices, edges)
        friends_cached = AM.getCachedDataFrame(friends)
        graph = GraphFrame(
            friends_cached,
            AM.getCachedDataFrame(edges),
        )

        for hop in range(degree):
            agg = graph.aggregateMessages(
                F.collect_list(AM.msg).alias("most_recently_visited"),
                sendToSrc=self.bump_udf(AM.dst["bfs"]),
                sendToDst=self.bump_udf(AM.src["bfs"]),
            )

            updated_friends = (
                graph.vertices.select("id", "bfs.*")
                .join(agg, ["id"], "inner")
                .withColumn(
                    "bfs",
                    self.update_shortest_path_udf(
                        "friends", "most_recently_visited"
                    ),
                )
            )["id", "bfs"]

            # Updating vertices. Using the approach to caching as in
            # https://graphframes.github.io/graphframes/docs/_site/api/python/_modules/graphframes/examples/belief_propagation.html
            friends_cached.unpersist()
            friends_cached = AM.getCachedDataFrame(updated_friends)
            graph = GraphFrame(friends_cached, graph.edges)

        return graph.vertices.select("id", "bfs.friends")

    @staticmethod
    def sort_and_clean(friendships):
        """Helper that removes self references in friendships, sorts friendships lexicographically and sorts rows by id.

        Args:
            friendships:

        Returns:
            +---+------------------------+
            |id |friends                 |
            +---+------------------------+
            |e  |{a -> 1, d -> 1, e -> 0}|
            |d  |{c -> 1, d -> 0, e -> 1}|
        """
        filter_and_sort_keys_udf = F.udf(
            lambda arr, key_not_in: sorted(
                [k for k in arr if k not in key_not_in]
            ),
            ArrayType(StringType()),
        )
        select_by_key_udf = F.udf(
            lambda d, keys: [d[k] for k in keys], ArrayType(ShortType())
        )
        return (
            friendships.withColumn(
                "keys", filter_and_sort_keys_udf(F.map_keys("friends"), "id")
            )
            .withColumn("distances", select_by_key_udf("friends", "keys"))
            .drop("friends")
            .withColumnRenamed("keys", "friends")
            .orderBy("id")
        )

    def find_friends(self, vertices, edges, degree: int):
        # friendships = self.pregel_port(vertices, edges, degree)
        friendships = self.bfs(vertices, edges, degree)
        return self.sort_and_clean(friendships)


def load_input_edges(spark: SparkSession, input_path: string):
    return (
        spark.read.option("delimiter", "\t").csv(input_path).toDF("src", "dst")
    )


def load_graph(edges) -> Tuple[DataFrame, DataFrame]:
    """

    Args:
        edges:

    Returns:
        two DataFrames, representing vertices and edges, respectively.
    """

    other_edges = edges.select(edges.dst.alias("src"), edges.src.alias("dst"))
    all_edges = edges.unionByName(other_edges)
    vertices = all_edges.select(all_edges.src.alias("id")).distinct()
    return vertices, edges


def save_as_text_file(df, include_degree, output):
    """
    Parses output of the graph algorithm and outputs a textual file.

    Args:
        df:
        include_degree: Controls whether to include or exclude degrees in the printout
        output: path to destination

    Returns:

    """

    vertices_and_distance_format_udf = F.udf(
        lambda vert, deg: [f"{k},{v}" for k, v in zip(vert, deg)],
        ArrayType(StringType()),
    )
    if include_degree:
        _injection = vertices_and_distance_format_udf("friends", "distances")
    else:
        _injection = "friends"
    return (
        df.select(
            F.concat(
                F.col("id"),
                F.lit("\t"),
                F.array_join(_injection, delimiter="\t"),
            ).alias("res")
        )
        .rdd.map(lambda x: x[0])
        .saveAsTextFile(output)
    )


def main():
    """
    Input file is not expectad to have a header. A header will get interpreted as an additional edge in your graph

    Returns:

    """
    parser = ArgumentParser()
    parser.add_argument("input", type=str)
    parser.add_argument("degree", type=int)
    parser.add_argument("output", type=str)
    parser.add_argument("--include-degree", type=bool, default=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp_dir:
        spark = init_spark(tmp_dir)
        ff = FindFriends()
        df = ff.find_friends(
            *load_graph(load_input_edges(spark, args.input)),
            degree=args.degree,
        )
        out = save_as_text_file(df, args.include_degree, args.output)

        spark.stop()

        return out


if __name__ == "__main__":
    main()
