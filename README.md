## saturator

[![Build Status](https://travis-ci.org/GlobalWebIndex/saturator.svg?branch=master)](https://travis-ci.org/GlobalWebIndex/saturator)

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
libraryDependencies += "net.globalwebindex" %% "saturator" % "x.y.x"
```
or
```
dependsOn(ProjectRef(uri("https://github.com/GlobalWebIndex/saturator.git#vx.y.x"), "saturator"))
```

See demo at `example/`. Akka persistence uses redis plugin as it is the best fit for saturator unless DAG gets really complex or
it has a lot of partitions in which case something like Cassandra would be a better fit.
It uses Kryo serialization because event log is persisted only temporarily and it would be deleted on new deploy.

```
$ cd docker
$ docker-compose up

```

It starts with a definition of a DAG which is a collection of edges, new partition interval and existing partitions in head Vertex.
Saturator FSM starts satisfying dependencies until it saturates DAG and then new partition is submitted periodically which yields a bunch of
unsatisfied dependencies, leaving DAG in unsaturated state -> FSM.