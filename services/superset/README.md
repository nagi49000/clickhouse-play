# Superset

Apache has set up a [docker compose for running superset](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/), with a number of built in datasets and dashboards.

### Apache's docker compose for superset

As described on the docs above, clone the repo containing the docker compose and related files
```
git clone --depth=1  https://github.com/apache/superset.git
```

From the superset folder, one can bring up a superset stack with
```
TAG=<image tag> docker compose -f docker-compose-image-tag.yml up
```
where `<image tag>` is a suitable tag as obtained from superset's dockerhub entries, e.f. `4.1.1`. The setup will probably be long and involved. Once the app is up and running (there should be some GET healthchecks in the docker output describing this), then the app should be available on `http://localhost:8088`.

### Other notes

The data source connector is SQLAlchemy; therefore superset can connect to anything, and only to data sources supplying a connection for SQLAlchemy. In practice, this means any data source that provides an SQL connector, and only sources that provide SQL connectors.