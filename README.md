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

### Use case

This system was designed to orchestrate an ETL pipeline or a system of microservices, it could be used together with [mawex](https://github.com/GlobalWebIndex/mawex)
as a task/job execution engine. Ie. your pipeline would let :
 - saturator decide what should be done
 - mawex take care about the actual ETL job or microservice execution

### how-to

```
libraryDependencies += "net.globalwebindex" %% "saturator" % "x.y.x"
```
or
```
dependsOn(ProjectRef(uri("https://github.com/GlobalWebIndex/saturator.git#vx.y.x"), "saturator-core"))
```

Note that this library is tested on [akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb) plugin, but running it on
different plugin like akka-persistence-redis is just a matter of configuration changes.

See demo at `example/` :

```
$ cd docker
$ docker-compose up

```

It starts with a definition of a DAG which is a collection of edges, new partition interval and existing partitions in head Vertex.
Saturator FSM starts satisfying dependencies until it saturates DAG and then new partition is submitted periodically which yields a bunch of
unsatisfied dependencies, leaving DAG in unsaturated state -> FSM.