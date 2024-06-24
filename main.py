import requests
from requests.auth import HTTPBasicAuth
import json

# Replace these with your actual username and password
username = 'Thang_XpOfq'
password = 'Winkbaifree_1908'

# The URL you want to send the request to
url = 'https://realtime.oxylabs.io/v1/queries'

# The request body
output_file = 'jewelry_for_women.json'

# Open the output file in write mode initially to create the file and write the opening bracket for JSON array
with open(output_file, 'w') as file:
    file.write('[\n')

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
            # Open the output file in append mode and write each result individually
            with open(output_file, 'a') as file:
                for result in results:
                    json.dump(result, file, indent=4)
                    file.write(',\n')
        else:
            print(f'No "results" field found in the response for iteration {i+1}.')
    else:
        print(f'Request failed with status code: {response.status_code} for iteration {i+1}.')

# Write the closing bracket for the JSON array
with open(output_file, 'a') as file:
    file.write('\n]')
    