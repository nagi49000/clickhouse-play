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

### Connecting to clickhouse from Superset

For clickhouse support, `clickhouse-connect` will need to be installed in each superset container.
```
docker exec superset_app pip install clickhouse-connect
docker exec superset_worker pip install clickhouse-connect
docker exec superset_worker_beat pip install clickhouse-connect
```

If the above pip installs need gcc, then for each of the above containers, run
```
docker exec <container name> apt upgrade
docker exec <container name> apt install -y gcc
```
and then re-run the pip installs.

An example for using the UI to connect to clickhouse and make charts is available on the [clickhouse docs](https://clickhouse.com/docs/en/integrations/superset).

On the app UI at `http://localhost:8088`, one should be able to go to (on the top right) 'Settings'>'Database Connections' to see a list of database connections, and then add a connection to clickhouse by clicking on '+ DATABASE', and clicking on the dropdown for 'supported databases', and click 'Clickhouse Connect (Superset)'. This indicates that the above pip installs have worked. To create the database connection, configure the host (ip address of your dev box, since [associated clickhouse service](../../docker-compose.yml) should be published on the host), the port to `8123`, and the database name to `db_random_user`.

One can then click on 'Datasets' in the top bar, and '+ DATASET' on the top right to add a dataset from the clickhouse database (database will be the clickhouse database, schema will be `db_random_user` and table will be `user`).

To add a chart for a dashboard, one can go to 'Charts' from the top ribbon, and click on '+ CHART'. For this data, one can make a world map chart, by choosing 'Map'>'World Map' in the 'Create a new chart' window. To get a count per country, one can add 'location_country' to the 'COUNTRY COLUMN' and specify the corresponding field type to be 'Full Name', and add a metric of 'COUNT(*)'.
