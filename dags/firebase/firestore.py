
from typing import Dict, Literal, Optional
from datetime import datetime, timezone


from .firestore_config import client

from .utils import DBUtils
from google.cloud.firestore_v1.base_query import FieldFilter


class Firestore():
    """
    This wrapper is an abstraction layer between the 
    application and the database.

    It provides a consistent interface to execute database
    operations, making it simpler to use, easier to maintain
    and avoiding code duplication.

    This encapsulation enhances code robustness, scalability, 
    and accelerates development.
    """

    @staticmethod
    def scan(collection: str):
        ref = client.collection(collection)
        docs = ref.stream()
        return DBUtils.filter_empty([DBUtils.parse_doc(doc) for doc in docs])

    @staticmethod
    def get_docs(collection: str, uid: str):
        ref = client.collection(collection)
        query = ref.where(filter=FieldFilter("uid", "==", uid))
        docs = query.stream()
        return DBUtils.filter_empty([DBUtils.parse_doc(doc) for doc in docs])

    @staticmethod
    def get_doc(collection: str, doc_id):
        doc_ref = client.collection(collection).document(doc_id)
        doc = doc_ref.get()

        if doc.exists:
            return doc.to_dict()
        return None

    @staticmethod
    def query(collection: str, key_values: Dict):
        """Returns all docs where the key-values match the query."""
        ref = client.collection(collection)
        for key, value in key_values.items():
            f = FieldFilter(key, "==", value)
            ref = ref.where(filter=f)
        docs = ref.stream()
        return DBUtils.filter_empty([DBUtils.parse_doc(doc) for doc in docs])

    @staticmethod
    def add(collection: str, data: Dict, id_key_name: str):
        """
        Inserts the document and adds new fields:
        - the "added_at" field
        - the newly created unique ID with the specified id_key_name.
        
        @param id_key_name: The name of the field that will store the ID
        of the document.
        Eg. "channel_id", "uid", "automation_id", etc.
        """
        time_now = datetime.now(timezone.utc).isoformat()
        data = {**data, "added_at": time_now}

        # 1. Insert
        ref = client.collection(collection)
        doc_id = ref.add(data)[1].id  # Firebase creates a unique ID and we use it

        # 2. Update (Add the ID to the document)
        ref.document(doc_id).update({ f"{id_key_name}": doc_id })

        return doc_id

    @staticmethod
    def add_with_existing_id(
        collection: str, 
        data: Dict, 
        id_key_name: str
    ):
        """
        Inserts the document and adds new the "added_at" field
        """
        time_now = datetime.now(timezone.utc).isoformat()
        data = {**data, "added_at": time_now}
        if id_key_name not in data:
            raise ValueError(f"ID key name {id_key_name} not found in the data.")
        # Insert with the existing ID
        ref = client.collection(collection).document(data[id_key_name])
        doc_id = ref.set(data)
        return doc_id

    @staticmethod
    def update(
        collection: str, 
        doc_id: str, 
        update_dict: Dict, 
        uid: Optional[str] = None
    ):
        updated = False
        try:
            ref = client.collection(collection).document(doc_id)
            if uid is not None:
                doc = ref.get()
                if doc.exists and doc.to_dict().get("uid") == uid:
                    ref.update(update_dict)
                    updated = True
            else:
                ref.update(update_dict)
                updated = True
        except Exception as e:
            print(f"Error updating Document with ID {doc_id}: {str(e)}")
        finally:
            return updated

    @staticmethod
    def update_where(
        collection: str,
        key_values: Dict,
        update_dict: Dict
    ):
        """Updates all documents where the key-values match the query."""
        updated = False
        try:
            ref = client.collection(collection)
            for key, value in key_values.items():
                f = FieldFilter(key, "==", value)
                ref = ref.where(filter=f)
            docs = ref.stream()
            for doc in docs:
                doc_ref = client.collection(collection).document(doc.id)
                doc_ref.update(update_dict)
            updated = True
        except Exception as e:
            print(
                f"Error updating Document with key-values {key_values}: {str(e)}")
        finally:
            return updated

    @staticmethod
    def delete(
        collection: str, 
        doc_id: str, 
        uid: Optional[str] = None
    ):
        """
        doc_id: The value of the document's ID.
        Eg. "abc123"
        """
        
        deleted = False
        try:
            ref = client.collection(collection).document(doc_id)
            if uid is not None:
                doc = ref.get()
                if doc.exists and doc.to_dict().get("uid") == uid:
                    ref.delete()
                    deleted = True
            else:
                ref.delete()
                deleted = True
        except Exception as e:
            print(f"Error deleting Document with ID {doc_id}: {str(e)}")
        finally:
            return deleted
        