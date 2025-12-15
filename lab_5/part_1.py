from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import string
import numpy as np
from random import random
import json

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

session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE};")
print(f"Keyspace '{KEYSPACE}' dropped")

session.execute(f"""
    CREATE KEYSPACE {KEYSPACE}
    WITH replication = {{
        'class': 'SimpleStrategy',
        'replication_factor': 3
    }};
""")
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
        PRIMARY KEY ((category), price, item_id, name, producer)
    ) WITH CLUSTERING ORDER BY (price ASC, item_id ASC, name ASC, producer ASC);
""")
print("Table 'items' created")

session.execute("""
    CREATE INDEX items_attributes_idx
    ON items (ENTRIES(attributes));
""")
print("Index on attributes created")

categories = ["steel", "aluminium", "concrete",
               "composite", "wood", "plastic", "other"]
def get_category():
    return categories[int(random()*len(categories))]
producers = ["IPT Inc.", "FizTech co.", "KPI and others",
               "Siko R Sky", "Mann. co.", "Aperture laboratories",
               "Walter White chemistry site", "Jack Horner Industrial pies",
               ""]
def get_producer():
    return producers[int(random()*len(producers))]

def get_name():
    name = ""
    name += string.ascii_uppercase[int(random()*len(string.ascii_uppercase))]
    for i in range(int(np.random.uniform(3, 16))):
        name += string.ascii_lowercase[int(random()*len(string.ascii_lowercase))]
    return name

def get_price():
    return round(np.random.uniform(0.5, 10**4), 2)

attributes = ["weight", "length", "width", "height"]

def get_atributes():
    att = dict()
    for i in attributes:
        if random() > 0.5:
            att[i] = str(round(np.random.uniform(1,1001),2))
    return att


for i in range(100):
    session.execute(
    "INSERT INTO items (category, item_id, name, price, producer, attributes) VALUES (%s, uuid(), %s, %s, %s, %s)",
    (get_category(), get_name(), get_price(), get_producer(), get_atributes())
)
    
print("Test data into items inserted")

input("Waiting for DESCRIBE")

print()
print("Category with sorting by price:")
rows = session.execute("SELECT * FROM items WHERE category=%s", ('aluminium',))
for row in rows:
    print(row)

print()
print("Category with name constrains:")
selected_name = input("Input name: ")
rows = session.execute("SELECT * FROM items WHERE category=%s AND name=%s", ('aluminium',selected_name,))
for row in rows:
    print(row)

print()
print("Category with price constrains (1000,5000):")
rows = session.execute("SELECT * FROM items WHERE category=%s AND price>1000 AND price<5000", ('aluminium',))
for row in rows:
    print(row)

print()
print("Category with price (1000,9000) and producer constrains :")
rows = session.execute("SELECT * FROM items WHERE category=%s AND price>1000 AND price<9000 AND producer = %s", ('aluminium',"IPT Inc.",))
for row in rows:
    print(row)

cluster.shutdown()

input("End")
