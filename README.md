## saturator

[![DroneCI](https://drone.globalwebindex.net/api/badges/GlobalWebIndex/saturator/status.svg)](https://drone.globalwebindex.net/GlobalWebIndex/saturator)
[![saturator-api](https://api.bintray.com/packages/l15k4/GlobalWebIndex/saturator-api/images/download.svg) ](https://bintray.com/l15k4/GlobalWebIndex/saturator-api/_latestVersion)
[![saturator-core](https://api.bintray.com/packages/l15k4/GlobalWebIndex/saturator-core/images/download.svg) ](https://bintray.com/l15k4/GlobalWebIndex/saturator-core/_latestVersion)

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
 - saturator decide what should be done based on changes in user system like new/created or changed partitions or outcome of job executions
 - mawex take care about the actual ETL job or microservice execution

### Saturator Flow

1. DagFSM is provided with initial state that represents state of the outer world (eg. partitioned storages)
2. DagFSM starts saturating (issuing commands to the user system) dependencies until all vertices in all partition DAGs are Complete and the FSM is idle
3. In the mean time, DagFSM asks user system for :
    - newly created partitions that are added as a new layer to partitioned DAG
    - partitions that changed in the user system so that it can re-evaluate particular partition DAG

### how-to

```
libraryDependencies += "net.globalwebindex" %% "saturator" % "x.y.x"
```
or
```
dependsOn(ProjectRef(uri("https://github.com/GlobalWebIndex/saturator.git#vx.y.x"), "saturator-core"))
```

Note that this library is tested on :
 - [akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb)
 - [akka-persistence-redis](https://github.com/safety-data/akka-persistence-redis)

But can run on different plugin like akka-persistence-cassandra, it is just a matter of how high your throughput is.

See demo at `example/` :

```
$ cd docker
$ docker-compose -f saturator-$plugin.yml up

```
