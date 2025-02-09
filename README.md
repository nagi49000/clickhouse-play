# clickhouse-play
A play area for using clickhouse as a data warehouse.

### Running

Services can be brought up from the root of the repo directory with
```
docker compose up --build
```

With the services up and running, data should be flowing into clickhouse. One can run a query to see how many rows are in clickhouse.
```
docker exec clickhouse-play-clickhouse-server-1 clickhouse-client --host localhost "SELECT COUNT(*) FROM db_random_user.user"
```