# Superset

Apache has set up a [docker compose for running superset](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/), with a number of built in datasets and dashboards.

### Apache's docker compose for superset

As described on the docs above, clone the repo containing the docker compose and related files
```
git clone --depth=1  https://github.com/apache/superset.git
```

### Other notes

The data source connector is SQLAlchemy; therefore superset can connect to anything, and only to data sources supplying a connection for SQLAlchemy. In practice, this means any data source that provides an SQL connector, and only sources that provide SQL connectors.