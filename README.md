K-hop neighborhood traversal (pyspark)
======================================

Input:
------

Input is a text file with each line specifying an edge between two graph nodes.

```
david  paula
david  kim
kim         tim
tim      paula
ann	    tim
watson       david
mary        watson
```

Output:
-------

A tab-separated file containing k-hop neighborhoods for each node in the graph.
Each line begins with a node and is followed by all k-hop neighbors of that node (together with
the minimum degree of the neighbor), sorted lexicographically.

```
ann    kim,2     paula,2       tim,1
david kim,1        mary,2       paula,1    tim,2 watson,1
kim        ann,2    david,1 paula,2    tim,1 watson,2
mary       david,2 watson,1
paula       ann,2    david,1 kim,2     tim,1 watson,2
tim    ann,1    david,2 kim,1     paula,1
watson      david,1 kim,2        mary,1    paula,2
```

Example usage:
------------

```
make run ARGS="./sample_data/mydata.csv 2 output.csv"
```
