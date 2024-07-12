import json
from flask import Flask, jsonify, Response
from pymongo import MongoClient
from bson import json_util, ObjectId
from flask import Flask, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from datetime import datetime, timedelta, timezone
from flask_cors import CORS
import diskcache as dc
from apscheduler.schedulers.background import BackgroundScheduler
import string
import pickle
import random
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.auth.exceptions import RefreshError
import google.auth.transport.requests
import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery
import os
import base64
from pyspark.sql import SparkSession
import pandas as pd



cache = dc.Cache('tmp_cache')
app = Flask(__name__)
cors = CORS(app, resources={
    r"/signup": {"origins": "http://localhost:5173"},
    r"/login": {"origins": "http://localhost:5173"},
    r"/forgot-password": {"origins": "http://localhost:5173"},
    r"/get-overview": {"origins": "http://localhost:5173"},
    r"/verify-otp": {"origins": "http://localhost:5173"},
    r"/get-sale-change": {"origins": "http://localhost:5173"},
})

CLIENT_SECRET_FILE = 'credentials.json'
TOKEN_PICKLE = 'token.pickle'

REDIRECT_URI = 'http://localhost:5000/callback'

MONGO_URI = "mongodb://localhost:27017/"
PRODUCT_COL = "product"
DATABASE_NAME = "admin"
DATABASE_STATISTIC_NAME = "amazon"

# JWT configuration
JWT_SECRET_KEY = 'jwt_secret_key'  # Replace with a strong secret key
JWT_ALGORITHM = 'HS256'
JWT_EXP_DELTA_SECONDS = 3600  # Access token expiration time (in seconds)
JWT_REFRESH_EXP_DELTA_SECONDS = 86400 

mongo_client = MongoClient(MONGO_URI) 
mongo_db = mongo_client[DATABASE_NAME]
product_collection = mongo_db[PRODUCT_COL]
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

def generate_otp(length=6):
    digits = string.digits
    otp = ''.join(random.choice(digits) for _ in range(length))
    return otp

def authenticate_gmail():
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    creds = None

    # Check if token.pickle file exists
    if os.path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, 'rb') as token:
            creds = pickle.load(token)

    # If no valid credentials available, prompt user to log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=2000)

        # Save the credentials for the next run
        with open(TOKEN_PICKLE, 'wb') as token:
            pickle.dump(creds, token)

    return creds

def send_email(service, user_id, message):
    try:
        message = (service.users().messages().send(userId=user_id, body=message).execute())
        print(f"Message Id: {message['id']}")
        return message
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

def create_message(sender, to, subject, message_text):
    message = MIMEText(message_text, 'html')
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    return {'raw': raw}
    
@app.route('/forgot-password', methods=['POST'])
def forgot_password():
    toEmail = request.json.get('email')

    if not toEmail:
        return jsonify({'message': 'Email are required.'}), 400
    
    user = user_collection.find_one({'email': toEmail})

    if not user:
        return jsonify({'message': 'Email .'}), 401
    
    creds = authenticate_gmail()
    service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)

    sender = "senderDoAn@gmail.com"
    subject = "Your Verification Code"
    otp = generate_otp()
    message_text = f"""\
<html>
<body style="font-family: Arial, sans-serif; margin: 0; padding: 0;">
  <div style="max-width: 600px; margin: auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;">
    <h2 style="color: #202124; font-size: 24px;">Verification Code</h2>
    <p>Dear {user.get("username")},</p>
    <p>Your OTP Code for accessing your account is:</p>
    <p style="font-size: 20px; font-weight: bold; color: #1a73e8;">{otp}</p>
    <p>Please enter this code in the provided field to complete your verification. This code is valid for the next 10 minutes.</p>
    <p>If you did not request this OTP, please ignore this email or contact our support team.</p>
    <p>Thank you,<br>Analytics</p>
  </div>
</body>
</html>
"""

    message = create_message(sender, toEmail, subject, message_text)
    send_email(service, "me", message)
    cache.set(toEmail, otp, 600)

    return jsonify({    
        'message': "OK", 
    }), 200

@app.route('/verify-otp', methods=['POST'])
def verify_otp():
    email = request.json.get('email')
    otp = request.json.get("otp")
    if email not in cache:
        return jsonify({'message': 'OTP expired.'}), 400
    if cache[email] != otp:
        return jsonify({'message': 'OTP wrong'}), 401
    user = user_collection.find_one({'email': email})

    if not user :
        return jsonify({'message': 'Email does not match any users.'}), 400
    access_token_payload = {
        'username': user.get("username"),
        'exp': datetime.now(timezone.utc) + timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }
    access_token = jwt.encode(access_token_payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    del cache[email]
    return jsonify({
        'access_token': access_token, 
        'username': user["username"],
        'role': user["role"]
    }), 200


@app.route('/get-sale-change', methods=['GET'])
def get_sale_change():
    query = {
        'timestamp': datetime(2024, 7, 12),
    }
    result0 = mongo_client["dashboard_01"]["controller"].find(query)
    query = {
        'timestamp': datetime(2024, 7, 11),
    }
    result1 = mongo_client["dashboard_01"]["controller"].find(query)


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

        # Fetch data from MongoDB
        query = {
            'category_code': {'$exists': True, '$ne': None},
            'event_time': {'$gte': from_date, '$lt': to_date + timedelta(1)}
        }
        projection = {'category_code': 1, 'event_time': 1}
        start = datetime.now()
        cursor = product_collection.find(query, projection)
        data = list(cursor)

        # Convert data to DataFrame
        df = pd.DataFrame(data)
        
        if df.empty:
            return Response(
                response=json.dumps({"error": "No data found for the given date range"}, default=custom_json_encoder),
                status=200,
                mimetype='application/json'
            )

        # Process data
        df['category'] = df['category_code'].apply(lambda x: x.split('.')[0])
        df['event_time'] = pd.to_datetime(df['event_time']).dt.strftime('%Y-%m-%d')
        categories = ['accessories', 'furniture', 'sport', 'appliances', 'electronics']
        df = df[df['category'].isin(categories)]
        result = df.groupby(['category', 'event_time']).size().reset_index(name='count')
        pivot_result = result.pivot(index='event_time', columns='category', values='count').fillna(0).astype(int)

        # Format the result for response
        res = pivot_result.to_dict(orient='list')
        print(datetime.now() - start)
        result = {
            "data": res,
            "labels": date_list
        }

        # Cache the result if needed
        
        return Response(
            response=json.dumps(result, default=custom_json_encoder),
            status=200,
            mimetype='application/json'
        )
    except Exception as e:
        return Response(
            response=json.dumps({"error": str(e)}),
            status=500,
            mimetype='application/json'
        )

if __name__ == '__main__':
    app.run()

