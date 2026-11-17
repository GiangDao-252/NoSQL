import os
from uuid import uuid4

from pymongo import MongoClient
from pymongo.errors import PyMongoError


ORDERS = [
    {
        "_id": 1001,
        "customer": {"name": "Linh", "city": "Hanoi"},
        "status": "paid",
        "items": [
            {"sku": "BK-01", "qty": 2, "price_cents": 1250},
            {"sku": "BK-07", "qty": 1, "price_cents": 1800},
        ],
    },
    {
        "_id": 1002,
        "customer": {"name": "Nam", "city": "Da Nang"},
        "status": "shipped",
        "items": [{"sku": "BK-03", "qty": 1, "price_cents": 2200}],
        "tags": ["gift"],
    },
    {
        "_id": 1003,
        "customer": {"name": "Mai", "city": "Hanoi"},
        "status": "paid",
        "items": [{"sku": "BK-01", "qty": 1, "price_cents": 1250}],
        "coupon": "WELCOME10",
    },
    {
        "_id": 1004,
        "customer": {"name": "Hung", "city": "Hue"},
        "status": "paid",
        "items": [{"sku": "BK-07", "qty": 2, "price_cents": 1800}],
        "delivery": {"method": "pickup"},
    },
    {
        "_id": 1005,
        "customer": {"name": "An", "city": "Da Nang"},
        "status": "pending",
        "items": [{"sku": "BK-01", "qty": 3, "price_cents": 1250}],
        "notes": "Awaiting payment",
    },
]


def find_orders_by_sku(orders, sku):
    print(f"Exercise 1 | Orders containing {sku}")
    orders.insert_many(ORDERS)
    for order in orders.find({"items.sku": sku}).sort("_id", 1):
        print(order)


def revenue_by_city(orders):
    # Count paid and shipped orders. Revenue is the sum of item subtotals.
    pipeline = [
        {"$match": {"status": {"$in": ["paid", "shipped"]}}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$customer.city",
                "revenue_cents": {
                    "$sum": {"$multiply": ["$items.qty", "$items.price_cents"]}
                },
            }
        },
        {"$sort": {"revenue_cents": -1, "_id": 1}},
    ]
    print("\nExercise 2 | Revenue by city")
    for result in orders.aggregate(pipeline):
        print(f"{result['_id']}: {result['revenue_cents'] / 100:.2f}")


def compare_index(orders):
    cities = ("Da Nang", "Hue", "Hanoi", "Can Tho")
    statuses = ("paid", "shipped", "pending", "cancelled")
    orders.insert_many(
        {
            "_id": number,
            "customer": {
                "name": f"Customer {number}",
                "city": cities[number % len(cities)],
            },
            "status": statuses[(number // len(cities)) % len(statuses)],
            "items": [{"sku": "BK-01", "qty": 1, "price_cents": 1250}],
        }
        for number in range(10_000)
    )

    query = {"customer.city": "Hanoi", "status": "paid"}
    print("\nExercise 3 | Index comparison on 10,000 orders")
    print(f"Filter: {query}")

    print_execution_stats(orders, query, "Before creating index")
    orders.create_index([("customer.city", 1), ("status", 1)])
    print_execution_stats(orders, query, "After creating index")


def print_execution_stats(orders, query, label):
    result = orders.database.command(
        "explain",
        {"find": orders.name, "filter": query},
        verbosity="executionStats",
    )
    stats = result["executionStats"]
    plan = result["queryPlanner"]["winningPlan"]

    print(f"\n{label}")
    print("Winning plan:", plan.get("queryPlan", plan))
    print("Documents returned:", stats["nReturned"])
    print("Documents examined:", stats["totalDocsExamined"])
    print("Index keys examined:", stats["totalKeysExamined"])
    print("Execution time (ms):", stats["executionTimeMillis"])


def main():
    uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        client.admin.command("ping")
        db = client.shop_lab

        # Use fresh collections so each run starts without a benchmark index.
        run_id = uuid4().hex
        orders = db[f"orders_{run_id}"]
        benchmark = db[f"orders_benchmark_{run_id}"]
        try:
            find_orders_by_sku(orders, "BK-01")
            revenue_by_city(orders)
            compare_index(benchmark)
        finally:
            benchmark.drop()
            orders.drop()


if __name__ == "__main__":
    try:
        main()
    except PyMongoError as error:
        raise SystemExit(f"MongoDB error: {error}") from None
