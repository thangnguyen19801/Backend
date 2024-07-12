import random
from pymongo import MongoClient
from datetime import datetime, timedelta
from pymongo import InsertOne

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Adjust the connection string as necessary
db = client['amazon']  # Replace with your database name
collection = db['laptop']  # Replace with your collection name
db1 = client['amazon_analyzed']  # Replace with your database name
collection1 = db1['laptop']  # Replace with your collection name

# Define the start and end datetime range
start_date = datetime(2024, 5, 1)
end_date = datetime.now()

# Function to generate a random datetime between two datetime objects
def random_date(start, end):
    delta = end - start
    int_delta = int(delta.total_seconds())
    random_second = random.randint(0, int_delta)
    return start + timedelta(seconds=random_second)

def get_collection_size(collection):
    stats = collection.database.command("collStats", collection.name)
    return stats.get("storageSize", 0)

# Find all records and convert the created_at field
target_size = 2 * 1024**3  # 2 GB in bytes

batch_size = 100000  # Number of documents to process in each batch
while get_collection_size(collection1) < target_size:
    batch = []
    for record in collection.find(no_cursor_timeout=True).batch_size(batch_size):
        if "content" in record and "results" in record["content"]:
            results = record["content"]["results"]
            if isinstance(results, dict):
                organics = results["organic"]
                amazon_choices = results["amazons_choices"]

                for organic in organics:
                    organic["timestamp"] = random_date(start_date, end_date)
                    batch.append(InsertOne(organic))

                for amazon_choice in amazon_choices:
                    amazon_choice["timestamp"] = random_date(start_date, end_date)
                    batch.append(InsertOne(amazon_choice))

                if len(batch) >= batch_size:
                    collection1.bulk_write(batch)
                    batch = []

    if batch:
        collection1.bulk_write(batch)

    print(f"Size {get_collection_size(collection1)} completed.")
    
print("Conversion complete.")
