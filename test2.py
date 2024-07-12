from pymongo import MongoClient
from datetime import datetime, timedelta

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.amazon_analyzed
db1 = client.dashboard_01

# Define the source and destination collections
source_collection_name = 'controller'  # Change this to your source collection name
destination_collection_name = 'controller'  # Changed to reflect daily records

# Define the date range
start_date = datetime(2024, 5, 1)
end_date = datetime(2024, 7, 12)
current_date = start_date

while current_date <= end_date:
    # Define the aggregation pipeline
    pipeline = [
        {
            '$match': {
                'timestamp': {
                    '$gte': current_date,
                    '$lt': current_date + timedelta(days=1),
                },
            },
        },
        {
            '$group': {
                '_id': None,
                'sales': {
                    '$sum': '$price',
                },
            },
        },
        {
            '$project': {
                '_id': 0,
                'sales': 1,
            },
        },
    ]

    # Execute the aggregation
    aggregation_result = db[source_collection_name].aggregate(pipeline)

    # Prepare the result for insertion
    result_list = list(aggregation_result)
    if result_list:
        result_list[0]["timestamp"] = current_date
        # Insert the aggregated result into the destination collection
        db1[destination_collection_name].insert_one(result_list[0])
        print(f"Inserted result for {current_date.date()} into {destination_collection_name}.")
    else:
        print(f"No documents found for {current_date.date()}.")

    # Move to the next day
    current_date += timedelta(days=1)

# Close the connection
client.close()
