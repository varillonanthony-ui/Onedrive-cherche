"""
Search engine — thin wrapper around Database for use in Streamlit.
"""
from db import Database


class SearchEngine:
    def __init__(self, db_path: str = "onedrive_index.db"):
        self.db = Database(db_path)

    def search(self, query: str, mode: str = "both", extensions: list = None, limit: int = 50) -> list[dict]:
        """
        Search the index.
        mode: 'name' | 'content' | 'both'
        extensions: list of ext strings without dot, e.g. ['pdf','docx'], or None for all
        """
        return self.db.search(query, mode=mode, extensions=extensions, limit=limit)

    def get_stats(self) -> dict:
        return self.db.get_stats()
