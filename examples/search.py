"""
Example demonstrating Redis Search functionality with the Upstash Python SDK.

This example shows how to:
- Create search indexes for different data types (string, JSON, hash)
- Index data with various field types (TEXT, numeric, boolean)
- Query with filters, sorting, and field selection
- Work with nested schemas
"""

import json
import dotenv
from upstash_redis import Redis

dotenv.load_dotenv()

# Initialize Redis client
redis = Redis.from_env()

# =============================================================================
# Example 1: Simple String Index with Product Catalog
# =============================================================================

print("=" * 60)
print("Example 1: String Index - Product Catalog")
print("=" * 60)

# Create an index for products stored as JSON strings
products_index = redis.search.create_index(
    name="products",
    dataType="string",  # Data is stored as strings
    prefix="product:",  # Index keys starting with "product:"
    schema={
        "name": "TEXT",  # Full-text searchable
        "category": {"type": "TEXT", "noTokenize": True},  # Exact match only
        "price": {"type": "F64", "fast": True},  # Float, optimized for sorting
        "stock": {"type": "U64", "fast": True},  # Unsigned integer
        "active": "BOOL",  # Boolean field
    }
)

# Add product data
products = [
    {
        "name": "Laptop Pro",
        "category": "electronics",
        "price": 1299.99,
        "stock": 50,
        "active": True,
    },
    {
        "name": "Laptop Basic",
        "category": "electronics",
        "price": 599.99,
        "stock": 100,
        "active": True,
    },
    {
        "name": "Wireless Mouse",
        "category": "electronics",
        "price": 29.99,
        "stock": 200,
        "active": True,
    },
    {
        "name": "USB Cable",
        "category": "accessories",
        "price": 9.99,
        "stock": 500,
        "active": False,
    },
]

for i, product in enumerate(products):
    redis.set(f"product:{i}", json.dumps(product))

# Wait for indexing to complete
products_index.wait_indexing()

# Query 1: Search for laptops
print("\n1. Search for 'Laptop' in name:")
results = products_index.query(filter={"name": {"$eq": "Laptop"}})
for result in results:
    print(f"  Key: {result['key']}, Score: {result['score']}")
    print(f"  Data: {result['data']}")

# Query 2: Find products over $500
print("\n2. Find products with price > 500:")
results = products_index.query(filter={"price": {"$gt": 500}})
for result in results:
    data = result["data"]
    print(f"  {data['name']}: ${data['price']}")

# Query 3: Get inactive products
print("\n3. Find inactive products:")
results = products_index.query(filter={"active": {"$eq": False}})
for result in results:
    data = result["data"]
    print(f"  {data['name']} (stock: {data['stock']})")

# Query 4: Search with sorting and field selection
print("\n4. Electronics sorted by price (descending), show only name and price:")
results = products_index.query(
    filter={"category": {"$eq": "electronics"}},
    orderBy={"price": "DESC"},
    select={"name": True, "price": True},
)
for result in results:
    data = result["data"]
    print(f"  {data['name']}: ${data['price']}")

# Query 5: Pagination
print("\n5. Pagination example (limit=2, offset=0 and offset=2):")
page1 = products_index.query(
    filter={"category": {"$eq": "electronics"}},
    limit=2,
    offset=0,
)
print(f"  Page 1: {len(page1)} results")
for result in page1:
    print(f"    - {result['data']['name']}")

page2 = products_index.query(
    filter={"category": {"$eq": "electronics"}},
    limit=2,
    offset=2,
)
print(f"  Page 2: {len(page2)} results")
for result in page2:
    print(f"    - {result['data']['name']}")

# Query 6: Count documents
print("\n6. Count electronics products:")
count_result = products_index.count(filter={"category": {"$eq": "electronics"}})
print(f"  Total electronics: {count_result['count']}")

# Query 7: Get only keys and scores (no content)
print("\n7. Get keys only (select={}):")
results = products_index.query(
    filter={"category": {"$eq": "electronics"}},
    select={},
    limit=2,
)
for result in results:
    print(f"  Key: {result['key']}, Score: {result['score']}")
    print(f"  Has data field: {'data' in result}")

# Describe the index
print("\n8. Index description:")
description = products_index.describe()
print(f"  Name: {description['name']}")
print(f"  Type: {description['dataType']}")
print(f"  Prefixes: {description['prefixes']}")
print(f"  Schema fields: {list(description['schema'].keys())}")

# =============================================================================
# Example 2: JSON Index with Nested Schema
# =============================================================================

print("\n" + "=" * 60)
print("Example 2: JSON Index - Blog Posts with Nested Data")
print("=" * 60)

# Create index with nested schema
posts_index = redis.search.create_index(
    name="posts",
    dataType="json",  # Data is stored as JSON
    prefix="post:",
    schema={
        "title": "TEXT",
        "author": {
            "name": "TEXT",
            "email": "TEXT",
        },
        "stats": {
            "views": {"type": "U64", "fast": True},
            "likes": {"type": "U64", "fast": True},
        },
        "published": "BOOL",
    }
)

# Add blog posts
posts = [
    {
        "title": "Getting Started with Redis",
        "author": {"name": "John Doe", "email": "john@example.com"},
        "stats": {"views": 1500, "likes": 75},
        "published": True,
    },
    {
        "title": "Advanced Redis Patterns",
        "author": {"name": "Jane Smith", "email": "jane@example.com"},
        "stats": {"views": 800, "likes": 40},
        "published": True,
    },
    {
        "title": "Redis Search Tutorial",
        "author": {"name": "John Doe", "email": "john@example.com"},
        "stats": {"views": 2000, "likes": 120},
        "published": True,
    },
]

for i, post in enumerate(posts):
    redis.json.set(f"post:{i}", "$", post)

posts_index.wait_indexing()

# Query nested fields
print("\n1. Posts by author 'John':")
results = posts_index.query(filter={"author.name": {"$eq": "John"}})
for result in results:
    data = result["data"]
    print(f"  Title: {data['title']}")
    print(f"  Author: {data['author']['name']}")
    print(f"  Views: {data['stats']['views']}")

# Query with nested numeric field
print("\n2. Posts with more than 1000 views:")
results = posts_index.query(
    filter={"stats.views": {"$gt": 1000}},
    orderBy={"stats.views": "DESC"},
)
for result in results:
    data = result["data"]
    print(f"  {data['title']}: {data['stats']['views']} views")

# Select nested fields
print("\n3. Get only author email and views count:")
results = posts_index.query(
    filter={"published": {"$eq": True}},
    select={"author.email": True, "stats.views": True},
    limit=2,
)
for result in results:
    data = result["data"]
    print(f"  Email: {data['author']['email']}, Views: {data['stats']['views']}")

# =============================================================================
# Example 3: Hash Index
# =============================================================================

print("\n" + "=" * 60)
print("Example 3: Hash Index - User Scores")
print("=" * 60)

# Create hash index
scores_index = redis.search.create_index(
    name="scores",
    dataType="hash",  # Data is stored as Redis hash
    prefix="user:",
    schema={
        "username": "TEXT",
        "score": {"type": "U64", "fast": True},
        "level": {"type": "U64", "fast": True},
    }
)

# Add user data using HSET
users = [
    {"username": "alice", "score": "9500", "level": "10"},
    {"username": "bob", "score": "8700", "level": "9"},
    {"username": "charlie", "score": "9200", "level": "10"},
]

for i, user in enumerate(users):
    redis.hset(f"user:{i}", values=user)

scores_index.wait_indexing()

# Query hash data
print("\n1. Top level 10 players:")
results = scores_index.query(
    filter={"level": {"$eq": 10}},
    orderBy={"score": "DESC"},
)
for result in results:
    data = result["data"]
    print(f"  {data['username']}: {data['score']} points")

# =============================================================================
# Example 4: Advanced Queries
# =============================================================================

print("\n" + "=" * 60)
print("Example 4: Advanced Query Features")
print("=" * 60)

# Fuzzy search (typo tolerance)
print("\n1. Fuzzy search for 'laptopp' (with typo):")
results = products_index.query(filter={"name": {"$fuzzy": "laptopp"}})
print(f"  Found {len(results)} results despite typo")
for result in results:
    print(f"    - {result['data']['name']}")

# Phrase search
print("\n2. Phrase search for 'Wireless Mouse':")
results = products_index.query(filter={"name": {"$phrase": "Wireless Mouse"}})
for result in results:
    print(f"  Found: {result['data']['name']}")

# Range query
print("\n3. Products priced between $10 and $100:")
results = products_index.query(
    filter={"price": {"$gte": 10, "$lte": 100}},
    orderBy={"price": "ASC"},
)
for result in results:
    data = result["data"]
    print(f"  {data['name']}: ${data['price']}")

# =============================================================================
# Cleanup
# =============================================================================

print("\n" + "=" * 60)
print("Cleanup")
print("=" * 60)

# Drop indexes
products_index.drop()
posts_index.drop()
scores_index.drop()

# Delete data
for i in range(len(products)):
    redis.delete(f"product:{i}")
for i in range(len(posts)):
    redis.delete(f"post:{i}")
for i in range(len(users)):
    redis.delete(f"user:{i}")

print("All indexes and data cleaned up!")
