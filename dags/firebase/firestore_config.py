# [LOCAL requirements to get type hinting]
# pip install google-cloud-firestore

# [CONFIGURATON for Firestore]
# From the Airflow UI, click on the Admin tab and select Connections.
# Edit the "google_cloud_default"
# Enter the "Keyfile Path" of the service account eg 
# "/home/airflow/gcs/dags/service_account.json"

# [INFO] the simplest way to connect to Firestore is to directly
# initialize the client with the credentials from the key file.
# The hook below doesn't allow to retrieve a collection.
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/firebase/hooks/firestore/index.html
# from airflow.providers.google.firebase.hooks.firestore import CloudFirestoreHook


import threading, os

from google.cloud import firestore
from google.oauth2 import service_account


class FirestoreClientSingleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    key_path = cls.get_json_path()
                    credentials = service_account.Credentials.from_service_account_file(key_path)
                    cls._instance = firestore.Client(credentials=credentials)
        return cls._instance
    
    @staticmethod
    def get_gcp_path():
        return '/home/airflow/gcs/dags/dustr-v2-firebase-adminsdk.json'
    
    @staticmethod
    def get_json_path():
        return os.path.join(os.path.dirname(__file__), '..', 'dustr-v2-firebase-adminsdk.json')

client = FirestoreClientSingleton()
"""Firestore client."""
