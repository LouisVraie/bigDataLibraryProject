# bigDataLibraryProject
Java application using MongoDB and ScyllaDB for library management.

The project is divided in 2 parts :
- MongoDB
- ScyllaDB

## Prerequisites

To do this project we used IntelliJ IDE, with Java 19 and Maven.

Also for the ScyllaDB part, we have used [Docker](https://hub.docker.com/r/scylladb/scylla).

```bash
docker run --name scylla -d scylladb/scylla
docker exec -it scylla nodetool status
```

```bash
docker exec -it scylla cqlsh
```


## MongoDB

To run the MongoDB part of the project, you need to run the `src/main/java/mongodb/Library.java` as the main program.

## ScyllaDB

To run the ScyllaDB part of the project, you need to run the `src/main/java/mongodb/Main.java` as the main program.

# Credits

- Louis Vraie
- Antoine Bruneau
- Amine Haddou
- Julien Lecocq