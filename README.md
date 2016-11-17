## saturator

Finite State Machine that is satisfying dependencies within partitioned/layered directed acyclic graph until it is fully saturated

```
                               +--+
                               |P1|
              +---------------->P2|
              |             V2 |X3|
              |                |  |                +--+
              |                +--+---------------->P1|
              |                                 V5 |X2|
              |                                    |X3|
            +-++               +------------------->  |            +--+
            |P1|               |P1|                +--+------------>P1|
         V1 |P2+--------------->X2|                             V8 |X2|
            |P3|            V3 |X3|                                |X3|
P4 --------->  |               |  |                +--------------->  |
            +-++               +--+                |P1|            +^-+
              |                                 V6 |P2|             |
              |                                    |X3|             |
              |                +------------------->  |             |
              |                |P1|                +--+             |
              +---------------->P2|                                 |
                            V4 |P3|                                 |
                               |  |                +----------------+
                               +--+---------------->P1|
                                                V7 |X2|
                                                   |X3|
                                                   |  |
                                                   +--+
```

- V1-8 = Graph Vertices
- P1-4 = Partitions
- X1-3 = Missing partitions, ie. graph edge P3 - X3 is a dependency that needs to be satisfied

### how-to

```
"net.globalwebindex" %% "saturator" % "0.01-SNAPSHOT"
```

See demo at `example/` :

```
$ cd docker
$ docker-compose up

```

It starts with a definition of a DAG which is a collection of edges, new partition interval and existing partitions in head Vertex.
Saturator FSM starts satisfying dependencies until it saturates DAG and then new partition is submitted periodically which yields a bunch of
unsatisfied dependencies, leaving DAG in unsaturated state -> FSM.