import json
from pymongo import MongoClient
from flask import Flask, jsonify, Response
from bson import json_util, ObjectId
from pymongo import MongoClient
from flask import Flask, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from datetime import datetime, timedelta, timezone
from flask_cors import CORS
import diskcache as dc
from apscheduler.schedulers.background import BackgroundScheduler

cache = dc.Cache('tmp_cache')
app = Flask(__name__)
cors = CORS(app, resources={
    r"/signup": {"origins": "http://localhost:5173"},
    r"/login": {"origins": "http://localhost:5173"},
    r"/get-overview": {"origins": "http://localhost:5173"},
})


MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "admin"
DATABASE_STATISTIC_NAME = "amazon"

# JWT configuration
JWT_SECRET_KEY = 'jwt_secret_key'  # Replace with a strong secret key
JWT_ALGORITHM = 'HS256'
JWT_EXP_DELTA_SECONDS = 3600  # Access token expiration time (in seconds)
JWT_REFRESH_EXP_DELTA_SECONDS = 86400 

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DATABASE_NAME]
product_collection = mongo_db["product"]
user_collection = mongo_db["user"]

def custom_json_encoder(data):
    return json.loads(json_util.dumps(data))

def cache_set(key, value, expire):
    cache[key] = {
        'value': value,
        'expire_at': datetime.now(timezone.utc) + timedelta(seconds=expire)
    }

def cache_has_valid_expire_time(key, max_remaining_seconds):
    if key in cache:
        expire_at = cache[key]['expire_at']
        remaining_seconds = (expire_at - datetime.now(timezone.utc)).total_seconds()
        return remaining_seconds <= max_remaining_seconds
    return False

def sync_data():
    for cache_key in cache.iterkeys():
        if cache_has_valid_expire_time(cache_key, 900):  # Check if key's expiration time is <= 15 minutes (900 seconds)
            print(f'Skipping sync for {cache_key}. Expiration time is still valid.')
            continue
        
        # Extract dates from cache key (assuming the key format is 'overview_key_from_to')
        if cache_key.startswith('overview_key_'):
            from_to_str = cache_key[len('overview_key_'):]
            from_date_str, to_date_str = from_to_str.split('_')
        else:
            print(f'Invalid cache key format: {cache_key}')
            continue
        
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        date_list = [
            date.strftime('%d-%m-%Y')  # Format date as day-month-year
            for date in (from_date + timedelta(n) for n in range((to_date - from_date).days + 1))
        ]

        pipeline = [
            {
                '$match': {
                    'category_code': {
                        '$exists': True, 
                        '$ne': None
                    }
                }
            },
            {
                '$project': {
                    'category': {
                        '$split': [
                            '$category_code', '.'
                        ]
                    }, 
                    'event_time': 1
                }
            },
            {
                '$project': {
                    'category': {
                        '$arrayElemAt': [
                            '$category', 0
                        ]
                    }, 
                    'event_time': 1
                }
            },
            {
                '$match': {
                    'event_time': {
                        '$gte': from_date, 
                        '$lt': to_date+timedelta(1)
                    }, 
                    'category': {
                        '$in': [
                            'accessories', 'furniture', 'sport', 'appliances', 'electronics'
                        ]
                    }
                }
            },
            {
                '$project': {
                    'event_time': {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': '$event_time'
                        }
                    }, 
                    'category': 1
                }
            },
            {
                '$group': {
                    '_id': {
                        'category': '$category', 
                        'event_time': '$event_time'
                    }, 
                    'count': {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort': {
                    '_id.category': 1, 
                    '_id.event_time': 1
                }
            },
            {
                '$group': {
                    '_id': '$_id.category', 
                    'data': {
                        '$push': '$count'
                    }
                }
            }
        ]

        data = list(product_collection.aggregate(pipeline))
        cache_set(cache_key, data, expire=3600)

        print(f'Cache updated for {cache_key} at {datetime.now()}')

@app.route('/', methods=['GET'])
def get_first_document():
    # Fetch the first document in the collection
    first_document = product_collection.find_one()
    if first_document:
        # Convert the document using the custom JSON encoder
        return Response(
            response=json.dumps(first_document, default=custom_json_encoder),
            status=200,
            mimetype='application/json'
        )
    else:
        return jsonify({"error": "No documents found"}), 404
    
@app.route('/signup', methods=['POST'])
def signup():
    username = request.json.get('username')
    password = request.json.get('password')
    email = request.json.get('email')

    if not username or not password or not email:
        return jsonify({'message': 'Username and password are required.'}), 400

    # Check if the username already exists
    existing_user = user_collection.find_one({'username': username})
    if existing_user:
        return jsonify({'message': 'Username already exists! Please choose a different one.'}), 400
    
    existing_user = user_collection.find_one({'email': email})
    if existing_user:
        return jsonify({'message': 'Email already exists! Please choose a different one.'}), 400

    # Hash the password before storing it
    password_hash = generate_password_hash(password, method='pbkdf2:sha256')

    # Create new user document
    new_user = {
        'username': username,
        'password_hash': password_hash,
        "role": "user",
        "email": email
    }
    try:
        user_collection.insert_one(new_user)
        return jsonify({'message': 'User created successfully.'}), 200
    except Exception as e:
        return jsonify({'message': f'Failed to create user: {str(e)}'}), 500

@app.route('/login', methods=['POST'])
def login():
    username = request.json.get('username')
    password = request.json.get('password')

    if not username or not password:
        return jsonify({'message': 'Username and password are required.'}), 400

    # Query the user by username
    user = user_collection.find_one({'username': username})

    if not user or not check_password_hash(user['password_hash'], password):
        return jsonify({'message': 'Invalid username or password.'}), 401

    # Generate access token
    access_token_payload = {
        'username': username,
        'exp': datetime.now(timezone.utc) + timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }
    access_token = jwt.encode(access_token_payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return jsonify({
        'access_token': access_token, 
        'username': user["username"],
        'role': user["role"]
    }), 200

@app.route('/logout')
def logout():
    if 'refresh_token' in session:
        session.pop('refresh_token')
        return jsonify({'message': 'Logged out successfully.'}), 200
    else:
        return jsonify({'message': 'Not logged in.'}), 500
    
@app.route('/get-statistic', methods=['GET'])
def get_statistic():
    category = request.json.get('category')
    collection = mongo_client[DATABASE_STATISTIC_NAME][category]

@app.route('/get-overview', methods=['GET'])
def get_overview():
    try:
            # Extract 'from' and 'to' parameters from request
            from_date_str = request.args.get('from')
            to_date_str = request.args.get('to')
            
            # Convert string parameters to datetime objects
            from_date = datetime.strptime(from_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            to_date = datetime.strptime(to_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            date_list = [
                date.strftime('%d-%m-%Y')  # Format date as day-month-year
                for date in (from_date + timedelta(n) for n in range((to_date - from_date).days + 1))
            ]

            # Generate a cache key based on 'from' and 'to' parameters
            cache_key = f'overview_key_{from_date_str}_{to_date_str}'

            # Check if result is cached
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                res = {record["_id"]: record["data"] for record in cached_result["value"]}
                result = {
                    "data": res,
                    "labels": date_list
                }
                return Response(
                    response=json.dumps(result, default=custom_json_encoder),
                    status=200,
                    mimetype='application/json'
                )

            # MongoDB aggregation pipeline
            pipeline = [
                {
                    '$match': {
                        'category_code': {
                            '$exists': True, 
                            '$ne': None
                        }
                    }
                },
                {
                    '$project': {
                        'category': {
                            '$split': [
                                '$category_code', '.'
                            ]
                        }, 
                        'event_time': 1
                    }
                },
                {
                    '$project': {
                        'category': {
                            '$arrayElemAt': [
                                '$category', 0
                            ]
                        }, 
                        'event_time': 1
                    }
                },
                {
                    '$match': {
                        'event_time': {
                            '$gte': from_date, 
                            '$lt': to_date+timedelta(1)
                        }, 
                        'category': {
                            '$in': [
                                'accessories', 'furniture', 'sport', 'appliances', 'electronics'
                            ]
                        }
                    }
                },
                {
                    '$project': {
                        'event_time': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$event_time'
                            }
                        }, 
                        'category': 1
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'category': '$category', 
                            'event_time': '$event_time'
                        }, 
                        'count': {
                            '$sum': 1
                        }
                    }
                },
                {
                    '$sort': {
                        '_id.category': 1, 
                        '_id.event_time': 1
                    }
                },
                {
                    '$group': {
                        '_id': '$_id.category', 
                        'data': {
                            '$push': '$count'
                        }
                    }
                }
            ]

            # Execute the pipeline
            data = list(product_collection.aggregate(pipeline))
            cache_set(cache_key, data, expire=3600)
            res = {record["_id"]: record["data"] for record in data}
            result = {
                "data": res,
                "labels": date_list
            }
            
            # Cache the result
            
            return Response(
                response=json.dumps(result, default=custom_json_encoder),
                status=200,
                mimetype='application/json'
            )

    except Exception as e:
        return Response(
            response=json.dumps({'error': str(e)}),
            status=500,
            mimetype='application/json'
        )

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(sync_data, 'interval', seconds=900)
    scheduler.start()
    try:
        app.run()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

