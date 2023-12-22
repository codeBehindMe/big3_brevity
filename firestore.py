from typing import Dict

from google.cloud import firestore as _firestore


class Firestore:
    def __init__(self, database: str) -> None:
        self.database_name = database
        self.db = _firestore.AsyncClient(database=database)

    async def add_document(
        self, collection_name: str, document_id: str, document_data: Dict
    ) -> None:
        d_ref = self.db.collection(collection_name).document(document_id)
        return await d_ref.set(document_data=document_data)
