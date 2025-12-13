from pymongo import MongoClient, WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.errors import PyMongoError
import datetime
import time

uri = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
client = MongoClient(uri)

db = client.get_database("testdb")

collection = db.get_collection(
    "test",
    write_concern=WriteConcern(w=3, wtimeout=10000)
)

doc = {
    "msg": "write concern 3 test",
    "ts": datetime.datetime.now()
}

print("Attempting insert")

try:
    result = collection.insert_one(doc)
    print("Write acknowledged:", result.inserted_id)
    inserted_id = result.inserted_id

except PyMongoError as e:
    print("WRITE FAILED:")
    print(type(e), str(e))

print("Attempting read")

collection_majority = db.get_collection(
    "test",
    read_concern=ReadConcern("majority")
)
time.sleep(1)

try:
    read_doc = collection_majority.find_one({"_id": inserted_id})
    print("Read result:", read_doc)

except PyMongoError as e:
    print("READ FAILED:")
    print(type(e), str(e))

print("Done.")
input("Press Enter to exit...")