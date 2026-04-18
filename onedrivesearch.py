import os\
import streamlit as st\
from msal import PublicClientApplication\
import requests\
import json\
from whoosh import index\
from whoosh.fields import Schema, TEXT, ID\
from whoosh.qparser import QueryParser\
from whoosh.analysis import StemmingAnalyzer\
import tempfile\
import PyPDF2\
import io\
from dotenv import load_dotenv\
\
# Charger les variables d'environnement\
load_dotenv()\
\
# Configuration Microsoft\
CLIENT_ID = os.getenv("f5cd8d4a-c26d-4862-8161-305535406d9e")\
TENANT_ID = os.getenv("2d27d05a-fa54-4c4d-a9ae-eff3c53e6656")\
AUTHORITY = f"https://login.microsoftonline.com/\{TENANT_ID\}"\
SCOPE = ["Files.Read.All"]\
REDIRECT_URI = "http://localhost:8501"\
\
# Configuration Whoosh\
INDEX_DIR = "whoosh_index"\
if not os.path.exists(INDEX_DIR):\
    os.makedirs(INDEX_DIR)\
\
# Sch\'e9ma de l'index Whoosh\
schema = Schema(\
    file_id=ID(stored=True),\
    file_name=TEXT(stored=True, analyzer=StemmingAnalyzer()),\
    file_path=TEXT(stored=True),\
    content=TEXT(stored=True, analyzer=StemmingAnalyzer()),\
    last_modified=TEXT(stored=True)\
)\
\
# Initialiser l'index Whoosh\
if not index.exists_in(INDEX_DIR):\
    ix = index.create_in(INDEX_DIR, schema)\
else:\
    ix = index.open_dir(INDEX_DIR)\
\
def get_msal_app():\
    return PublicClientApplication(\
        client_id=CLIENT_ID,\
        authority=AUTHORITY,\
        token_cache=None\
    )\
\
def authenticate():\
    app = get_msal_app()\
    result = app.acquire_token_interactive(\
        scopes=SCOPE,\
        redirect_uri=REDIRECT_URI\
    )\
    return result\
\
def get_access_token():\
    app = get_msal_app()\
    accounts = app.get_accounts()\
    if accounts:\
        result = app.acquire_token_silent(SCOPE, account=accounts[0])\
        if result:\
            return result['access_token']\
    return None\
\
def extract_text_from_pdf(file_content):\
    try:\
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))\
        text = ""\
        for page in pdf_reader.pages:\
            text += page.extract_text()\
        return text\
    except:\
        return ""\
\
def index_files(access_token):\
    headers = \{\
        "Authorization": f"Bearer \{access_token\}",\
        "Content-Type": "application/json"\
    \}\
\
    # R\'e9cup\'e9rer la liste des fichiers depuis OneDrive\
    response = requests.get(\
        "https://graph.microsoft.com/v1.0/me/drive/root/children",\
        headers=headers\
    )\
\
    if response.status_code != 200:\
        st.error("Erreur lors de la r\'e9cup\'e9ration des fichiers OneDrive")\
        return\
\
    files = response.json().get("value", [])\
\
    writer = ix.writer()\
\
    for file in files:\
        if file["name"].lower().endswith(('.pdf', '.txt')):\
            file_id = file["id"]\
            file_name = file["name"]\
            file_path = file.get("webUrl", "")\
\
            # T\'e9l\'e9charger le contenu du fichier\
            file_response = requests.get(\
                f"https://graph.microsoft.com/v1.0/me/drive/items/\{file_id\}/content",\
                headers=headers\
            )\
\
            if file_response.status_code == 200:\
                content = ""\
                if file_name.lower().endswith('.pdf'):\
                    content = extract_text_from_pdf(file_response.content)\
                elif file_name.lower().endswith('.txt'):\
                    content = file_response.text\
\
                # Ajouter \'e0 l'index\
                writer.add_document(\
                    file_id=file_id,\
                    file_name=file_name,\
                    file_path=file_path,\
                    content=content,\
                    last_modified=file.get("lastModifiedDateTime", "")\
                )\
\
    writer.commit()\
    st.success("Indexation termin\'e9e avec succ\'e8s!")\
\
def search_files(query):\
    with ix.searcher() as searcher:\
        parser = QueryParser("content", ix.schema)\
        parsed_query = parser.parse(query)\
        results = searcher.search(parsed_query, limit=20)\
\
        if not results:\
            st.warning("Aucun r\'e9sultat trouv\'e9")\
            return []\
\
        return [\{\
            "name": hit["file_name"],\
            "path": hit["file_path"],\
            "content": hit.highlights("content") or hit["content"][:200] + "..."\
        \} for hit in results]\
\
def main():\
    st.title("Recherche OneDrive")\
\
    # Authentification\
    if "auth_result" not in st.session_state:\
        st.session_state.auth_result = None\
\
    if st.button("Se connecter \'e0 OneDrive"):\
        st.session_state.auth_result = authenticate()\
\
    if st.session_state.auth_result:\
        st.success("Connect\'e9 avec succ\'e8s!")\
\
        # Obtenir le token d'acc\'e8s\
        access_token = get_access_token()\
        if not access_token:\
            st.error("Erreur d'authentification")\
            return\
\
        # Indexation\
        if st.button("Indexer les fichiers OneDrive"):\
            with st.spinner("Indexation en cours..."):\
                index_files(access_token)\
\
        # Recherche\
        query = st.text_input("Rechercher dans les fichiers:")\
        if query:\
            results = search_files(query)\
            for result in results:\
                st.subheader(result["name"])\
                st.markdown(f"[Ouvrir dans OneDrive](\{result['path']\})")\
                st.markdown(f"**Contenu:** \{result['content']\}")\
                st.divider()\
\
if __name__ == "__main__":\
    main()\
}
