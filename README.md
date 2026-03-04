clusqlite

[Donate](https://www.paypal.com/ncp/payment/KDNN5CHG4C3RN)

simple distributed sqlite

clusqlite example:

Run 3 nodes like this:

# follower 1
python dist_sqlite_sql.py \\
  --node-id n2 \\
  --role follower \\
  --port 8002 \\
  --db data/n2.db

# follower 2
python dist_sqlite_sql.py \\
  --node-id n3 \\
  --role follower \\
  --port 8003 \\
  --db data/n3.db

# leader
python dist_sqlite_sql.py \\
  --node-id n1 \\
  --role leader \\
  --port 8001 \\
  --db data/n1.db \\
  --peers http://127.0.0.1:8002,http://127.0.0.1:8003


# Usage

Create schema

curl -X POST http://127.0.0.1:8001/execute \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, age INTEGER NOT NULL)"
  }'

Insert rows

curl -X POST http://127.0.0.1:8001/execute \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "INSERT INTO users (name, age) VALUES (?, ?)",
    "params": ["alice", 30]
  }'
Update rows

curl -X POST http://127.0.0.1:8001/execute \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "UPDATE users SET age = ? WHERE name = ?",
    "params": [31, "alice"]
  }'

Multi-statement transaction

curl -X POST http://127.0.0.1:8001/execute \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [
      {
        "sql": "INSERT INTO users (name, age) VALUES (?, ?)",
        "params": ["bob", 25]
      },
      {
        "sql": "INSERT INTO users (name, age) VALUES (?, ?)",
        "params": ["carol", 28]
      },
      {
        "sql": "CREATE INDEX idx_users_name ON users(name)",
        "params": []
      }
    ]
  }'

Read from leader or follower

curl -X POST http://127.0.0.1:8002/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT id, name, age FROM users ORDER BY id",
    "params": []
  }'
