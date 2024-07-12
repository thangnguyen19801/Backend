import requests
from requests.auth import HTTPBasicAuth
import pymongo
from pymongo import MongoClient
import random
from datetime import datetime, timedelta
from pymongo import InsertOne

# Replace these with your actual username and password
username = 'Thang_XpOfq'
password = 'Winkbaifree_1908'

# The URL you want to send the request to
url = 'https://realtime.oxylabs.io/v1/queries'

# MongoDB connection details
mongo_uri = 'mongodb://localhost:27017/'  # Update this with your MongoDB URI if necessary
db_name = 'amazon_analyzed'
collection_name = 'jewelry_for_women'


# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

db1 = client['amazon_analyzed']  # Replace with your database name
collection1 = db1['laptop']  # Replace with your collection name

start_date = datetime(2024, 5, 1)
end_date = datetime.now()

def random_date(start, end):
    delta = end - start
    int_delta = int(delta.total_seconds())
    random_second = random.randint(0, int_delta)
    return start + timedelta(seconds=random_second)

# Loop through 100 iterations to get 1000 pages (10 pages per iteration)
for i in range(100):
    # Define the payload with the updated start_page for each iteration
    payload = {
        "source": "amazon_search",
        "query": "jewelry for women",
        "domain": "com",
        "geo_location": "90210",
        "start_page": i * 10 + 1,
        "parse": True,
        "pages": 10
    }
    
    # Make the POST request with basic authentication and a request body
    response = requests.post(url, auth=HTTPBasicAuth(username, password), json=payload)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        response_json = response.json()
        
        # Extract the 'results' field
        results = response_json.get('results', None)
        
        if results is not None:
            # Insert each result into MongoDB
            for record in results:
                if "content" in record and "results" in record["content"]:
                    results = record["content"]["results"]
                    if isinstance(results, dict):
                        organics = results["organic"]
                        amazon_choices = results["amazons_choices"]

                        for organic in organics:
                            organic["timestamp"] = random_date(start_date, end_date)
                            collection1.insert_one(organic)

                        for amazon_choice in amazon_choices:
                            amazon_choice["timestamp"] = random_date(start_date, end_date)
                            collection1.insert_one(amazon_choice)
            print(f'Inserted {len(results)} documents for iteration {i+1}.')
        else:
            print(f'No "results" field found in the response for iteration {i+1}.')
    else:
        print(f'Request failed with status code: {response.status_code} for iteration {i+1}.')

# Close the MongoDB connection
client.close()
