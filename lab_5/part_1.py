from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import string
import numpy as np
import random
from datetime import datetime, timezone

KEYSPACE = "lab_keyspace"

try:
    print("Trying to reach Cassandra node 1")
    cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
    session = cluster.connect()
except:
    try:
        print("Trying to reach Cassandra node 2")
        cluster = Cluster(contact_points=["127.0.0.1"], port=9043)
        session = cluster.connect()
    except:
        try:
            print("Trying to reach Cassandra node 3")
            cluster = Cluster(contact_points=["127.0.0.1"], port=9044)
            session = cluster.connect()
        except:
            raise Exception("Cannot find cluster")

session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE};", timeout=30.0)
print(f"Keyspace '{KEYSPACE}' dropped")


session.execute(f"""
    CREATE KEYSPACE {KEYSPACE}
    WITH replication = {{
        'class': 'SimpleStrategy',
        'replication_factor': 3
    }};
""", timeout=30.0)
print(f"Keyspace '{KEYSPACE}' created")

session.set_keyspace(KEYSPACE)


session.execute("""
    CREATE TABLE items (
        category text,
        item_id uuid,
        name text,
        price decimal,
        producer text,
        attributes map<text, text>,
        PRIMARY KEY ((category), price, item_id)
    ) WITH CLUSTERING ORDER BY (price ASC, item_id ASC);
""", timeout=30.0)
print("Table 'items' created")

session.execute("""
    CREATE INDEX items_attributes_idx
    ON items (ENTRIES(attributes));
""", timeout=30.0)
print("Index on attributes created")

categories = ["steel", "aluminium", "concrete",
               "composite", "wood", "plastic", "other"]
def get_category():
    return categories[int(random.random()*len(categories))]
producers = ["IPT Inc.", "FizTech co.", "KPI and others",
               "Siko R Sky", "Mann. co.", "Aperture laboratories",
               "Walter White chemistry site", "Jack Horner Industrial pies"]
def get_producer():
    return producers[int(random.random()*len(producers))]

def get_name():
    name = ""
    name += string.ascii_uppercase[int(random.random()*len(string.ascii_uppercase))]
    for i in range(int(np.random.uniform(3, 16))):
        name += string.ascii_lowercase[int(random.random()*len(string.ascii_lowercase))]
    return name

def get_price():
    return round(np.random.uniform(0.5, 10**4), 2)

attributes = ["weight", "length", "width", "height"]

def get_atributes():
    att = dict()
    for i in attributes:
        if random.random() > 0.5:
            att[i] = str(round(np.random.uniform(1,1001),2))
    return att

for i in range(100):
    session.execute(
    "INSERT INTO items (category, item_id, name, price, producer, attributes) VALUES (%s, uuid(), %s, %s, %s, %s)",
    (get_category(), get_name(), get_price(), get_producer(), get_atributes(),), timeout=30.0)
print("Test data into items inserted")


session.execute("""
    CREATE TABLE orders (
        customer_name text,
        order_date timestamp,
        order_id uuid,
        item_ids list<uuid>,
        total_price decimal,
        PRIMARY KEY ((customer_name), order_date, order_id)
    ) WITH CLUSTERING ORDER BY (order_date DESC);
""", timeout=30.0)
print("Table 'orders' created")

customers = ["RED", "BLU", "Baldwin locomotive company",
             "Depressing coal mines", 
             "Slightly less depressing coal mines",
             "Construction company that constructs offices for construction companies",
             "O.W.C.A",
             "Doofenshmirtz Evil Inc.",
             "This company does not build secret goverment progects Inc."]

def get_customer():
    return customers[int(random.random()*len(customers))]

def get_order_date(min = 2015, max = 2025):
    start = datetime(min, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(max, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())

    random_ts = random.randint(start_ts, end_ts)

    return datetime.fromtimestamp(random_ts, tz=timezone.utc)

def get_all_items_ids_and_prices():
    ids = []
    prices = []
    for category in categories:
        rows = session.execute("SELECT item_id, price FROM items WHERE category=%s", (category,), timeout=30.0)
        for row in rows:
            ids.append(row.item_id)
            prices.append(row.price)
    return ids, prices

ids, prices = get_all_items_ids_and_prices()

def get_price(uuid):
    try:
        i = ids.index(uuid)
    except:
        return False
    return prices[i]

def get_items_and_price(ids):
    items = np.random.choice(ids, size=int(np.random.uniform(1, 16)), replace=False).tolist()
    price = sum([get_price(uuid) for uuid in items])
    return items, price
        
for i in range(100):
    items, price = get_items_and_price(ids)
    session.execute(
    "INSERT INTO orders (customer_name, order_date, order_id, item_ids, total_price) VALUES (%s, %s, uuid(), %s, %s)",
    (get_customer(), get_order_date(), items, price,), timeout=30.0)
print("Test data into orders inserted")


input("Waiting for manual DESCRIBE")

print()
print("Category with sorting by price:")
rows = session.execute("SELECT * FROM items WHERE category=%s", ('aluminium',), timeout=30.0)
for row in rows:
    print(row)


print()
try:
    session.execute("CREATE INDEX name_index ON items(name)")
except:
    print("Failed to create index on name (May already exist)")
print("Category with name constrains:")
selected_name = input("Input name: ")
rows = session.execute("SELECT * FROM items WHERE category=%s AND name = %s", ('aluminium',selected_name,), timeout=30.0)
for row in rows:
    print(row)

print()
print("Category with price constrains (1000,5000):")
rows = session.execute("SELECT * FROM items WHERE category=%s AND price>1000 AND price<5000", ('aluminium',), timeout=30.0)
for row in rows:
    print(row)

print()
try:
    session.execute("CREATE INDEX producer_index ON items(producer)", timeout=30.0)
except:
    print("Failed to create index on producer (May already exist)")
print("Category with price (1000,9000) and producer constrains :")


for producer in producers:
    rows = session.execute("SELECT * FROM items WHERE category=%s AND price>1000 AND price<9000 AND producer = %s", ('aluminium',producer,), timeout=30.0)
    producer_found = False
    for row in rows:
        producer_found = True
        print(row)
    if producer_found:
        break


print()
print("Orders for RED with sorting by date:")
rows = session.execute("SELECT * FROM orders WHERE customer_name=%s", ('RED',), timeout=30.0)
for row in rows:
    print(row)

print()
print("All orders total price for every customer:")
rows = session.execute("SELECT DISTINCT customer_name FROM orders", timeout=30.0)
customer_names = [row.customer_name for row in rows]
for customer in customer_names:
    rows = session.execute("SELECT total_price FROM orders WHERE customer_name=%s", (customer,), timeout=30.0)
    total = sum([row.total_price for row in rows])
    print(customer, total)

print()
print("WRITETIME:")
rows = session.execute("""SELECT order_id, customer_name, WRITETIME(total_price) AS price_write_time
        FROM orders WHERE customer_name = %s""", ('RED',), timeout=30.0)
for row in rows:
    print(row)

cluster.shutdown()

input("End")
