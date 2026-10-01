# 🎲 Data Generator

Simulates shop activity on the BikeStores source Postgres so CDC keeps flowing through
bronze → silver → gold. It only changes the four tables Debezium captures.

## Actions

Every tick runs one weighted random action in a single transaction:

| Action | Weight | Tables | CDC events |
|--------|--------|--------|------------|
| Place an order (1–3 in-stock items, stock decreases) | 45% | `orders`, `order_items`, `stocks` | c, c, u |
| Advance an order (pending → processing → completed 90% / rejected 10%) | 30% | `orders` | u |
| New customer (Faker) | 10% | `customers` | c |
| Customer moves address | 10% | `customers` | u |
| Restock a product (+5–20) | 5% | `stocks` | u |

Existing stores, staff, products and prices are reused; IDs come from Postgres sequences.

## Configuration

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | ✅ | — | Source DB credentials (from `.env`) |
| `GENERATOR_INTERVAL_SECONDS` | | `5` | Seconds between actions |
| `GENERATOR_SEED` | | — | Repeat the same sequence of actions (given the same DB state) |

Connects to `postgres:5432` inside the `data-platform` Docker network.

## Run

From `docker/`, with Postgres up:

```bash
docker build -t data-generator ./data-generator
docker run --rm --network data-platform --env-file ../.env data-generator
```

Each action is logged, e.g. `2026-10-01 10:00:05 new order 1617 (2 items) for customer 259`.
