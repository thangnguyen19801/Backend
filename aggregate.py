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

MONGO_URI = "mongodb://localhost:27017/"
PRODUCT_COL = "product"
DATABASE_NAME = "admin"
DATABASE_STATISTIC_NAME = "amazon"

mongo_client = MongoClient(MONGO_URI) 
mongo_db = mongo_client[DATABASE_NAME]
product_collection = mongo_db[PRODUCT_COL]
user_collection = mongo_db["user"]

products = product_collection.find()

# Print the products
pipeline = [
    {
        '$group': {
            '_id': '$category_code'
        }
    },
    {
        '$project': {
            '_id': 0, 
            'category_code_0': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$_id', '.'
                        ]
                    }, 0
                ]
            }
        }
    },
    {
        '$group': {
            '_id': '$category_code_0'
        }
    },
    {
        '$match': {
            '_id': {
                '$ne': None
            }
        }
    }
]

results = list(product_collection.aggregate(pipeline))
print(results)

        
# Print each result
for result in results:
    new_collection = result.get("_id")
    print(new_collection)
    cursor = product_collection.aggregate([
    {
        '$project': {
            'category': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$category_code', '.'
                        ]
                    }, 0
                ]
            }, 
            'classification': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$category_code', '.'
                        ]
                    }, 1
                ]
            }, 
            'event_time': 1, 
            'brand': 1, 
            'price': 1, 
            'event_type': 1
        }
    }, {
        '$match': {
            'category': new_collection
        }
    }, {
        '$project': {
            'event_time': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$event_time'
                }
            }, 
            'event_type': 1, 
            'brand': 1, 
            'price': 1, 
            'classification': 1
        }
    }, {
        '$project': {
            'event_time': {
                '$dateFromString': {
                    'dateString': '$event_time', 
                    'format': '%Y-%m-%d'
                }
            }, 
            'event_type': 1, 
            'brand': 1, 
            'price': 1, 
            'classification': 1
        }
    }, {
        '$group': {
            '_id': {
                'event_time': '$event_time', 
                'classification': '$classification', 
                'brand': '$brand'
            }, 
            'view': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$event_type', 'view'
                            ]
                        }, 1, 0
                    ]
                }
            }, 
            'cart': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$event_type', 'cart'
                            ]
                        }, 1, 0
                    ]
                }
            }, 
            'purchase': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$event_type', 'purchase'
                            ]
                        }, 1, 0
                    ]
                }
            }, 
            'price': {
                '$push': '$price'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'event_time': '$_id.event_time', 
            'brand': '$_id.brand', 
            'classification': '$_id.classification', 
            'view': 1, 
            'cart': 1, 
            'purchase': 1, 
            'price': 1
        }
    }, {
        '$sort': {
            'event_time': 1, 
            'classification': 1, 
            'brand': 1
        }
    }
    ]
    )
    for document in cursor:
        print(document)
        mongo_db[new_collection].insert_one(document)
