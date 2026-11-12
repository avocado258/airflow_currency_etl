# Currency Exchange Rate ETL Pipeline (Airflow + MongoDB)

Daily ETL pipeline built with Astro CLI (Apache Airflow). It pulls EUR exchange rates from the [Frankfurter API](https://frankfurter.dev/), transforms them, and loads them into MongoDB. Mongo Express provides a web UI for the database.

## Architecture

```
Frankfurter API  ->  extract  ->  transform  ->  load  ->  MongoDB (currency_db.exchange_rates)
                        |            |            |
                     XCom key     XCom key     reads XCom
                    raw_rates  transformed_rates
```

| Task | Description | XCom |
|------|-------------|------|
| `extract` | GET `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY` | pushes `raw_rates` |
| `transform` | Reshapes each rate into the target schema | pulls `raw_rates`, pushes `transformed_rates` |
| `load` | Upserts documents into MongoDB | pulls `transformed_rates` |

Tasks exchange data through named XCom keys (`ti.xcom_push(key=...)` / `ti.xcom_pull(key=..., task_ids=...)`).

### Transformed document schema

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1117,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T07:15:00"
}
```

## Project structure

```
.
├── dags/
│   └── currency_etl.py              # ETL DAG (extract >> transform >> load)
├── docker-compose.override.yml      # MongoDB + Mongo Express services
├── requirements.txt                 # requests, pymongo
├── Dockerfile                       # Astro runtime image
└── README.md
```

## Docker configuration

- **Networking:** `mongo` and `mongo-express` join the Astro `airflow` bridge network, so the DAG reaches the database at hostname `mongo` (not `localhost`).
- **Persistent storage:** MongoDB data is stored in the named volume `mongo_data` (mounted at `/data/db`), so data survives container restarts.
- Both services carry `meta.networking` / `meta.storage` labels documenting the above.

## Prerequisites

- Docker Desktop running (Linux containers; WSL2 backend on Windows)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`astro version`)
- Git

## Setup and run

```bash
git clone https://github.com/avocado258/airflow_currency_etl
cd airflow_currency_etl
astro dev start
```

| Service | URL | Login |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| Mongo Express | http://localhost:8081 | `admin` / `pass` (default; override with `ME_CONFIG_BASICAUTH_USERNAME` / `ME_CONFIG_BASICAUTH_PASSWORD`) |
| MongoDB | `mongodb://admin:admin123@mongo:27017/` (inside Docker network) | root user `admin` |

1. In the Airflow UI, unpause the `currency_etl` DAG and trigger it (it is also scheduled `@daily`).
2. Check XComs: **Browse -> XComs**, or Dags -> `currency_etl` -> run -> task -> **XCom** tab.
3. Check data: Mongo Express -> `currency_db` -> `exchange_rates`.

## Verification commands

```bash
docker ps
docker volume ls
docker network ls
docker network inspect <project>_<id>_airflow     # should list mongo and mongo-express

# document count from the shell
docker exec mongo mongosh -u admin -p admin123 --authenticationDatabase admin \
  --eval "db.getSiblingDB('currency_db').exchange_rates.countDocuments()"
```

### Persistence test

```bash
astro dev stop
astro dev start
```

The documents in `currency_db.exchange_rates` remain. Do not use `astro dev kill`, which removes volumes.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `failed to connect to the docker API` | Start Docker Desktop and wait for "Engine running"; verify with `docker info` |
| DAG not listed | `astro dev run dags list-import-errors`; check `dags/currency_etl.py` and `requirements.txt`, then `astro dev restart` |
| `Connection refused` in `load` | Use host `mongo`, and confirm `mongo` is on the `airflow` network |
| Port 8080/8081 in use | Stop the conflicting process or change the host port mapping |
| New dependency not found | Add it to `requirements.txt`, then `astro dev restart` |

## Data source

[Frankfurter API](https://frankfurter.dev/) - free, public currency exchange-rate API.