
from typing import Any, Dict, List, Optional

from google.cloud.firestore_v1.document import DocumentSnapshot


class DBUtils():

    @staticmethod
    def parse_doc(doc: DocumentSnapshot):
        """Parse the doc to a dict and populates the id field of a document."""
        doc_dict = doc.to_dict()
        doc_dict['id'] = doc.id
        return doc_dict
    
    @staticmethod
    def filter_empty(docs: List[Optional[Dict[str, Any]]]) \
        -> List[Dict[str, Any]]:
        return [doc for doc in docs if doc is not None]
