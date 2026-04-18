# 🔍 OneDrive Search

Application Streamlit pour indexer et rechercher dans l'intégralité de votre OneDrive (noms de fichiers + contenu).

Supporte : **PDF, Word (.docx), Excel (.xlsx), PowerPoint (.pptx), CSV, TXT, Markdown** et plus.

---

## 🏗️ Architecture

```
onedrive-search/
├── app.py              # Interface Streamlit
├── onedrive_client.py  # Client Microsoft Graph API (auth MSAL)
├── indexer.py          # Crawling OneDrive + extraction texte
├── db.py               # Base SQLite avec FTS5 (full-text search)
├── search.py           # Moteur de recherche
├── requirements.txt
└── .streamlit/
    └── config.toml
```

---

## 🚀 Installation locale

```bash
git clone https://github.com/VOTRE_USER/onedrive-search
cd onedrive-search
pip install -r requirements.txt
streamlit run app.py
```

---

## ☁️ Déploiement sur Streamlit Cloud (gratuit)

1. **Forkez** ce repo sur GitHub
2. Allez sur [share.streamlit.io](https://share.streamlit.io)
3. Connectez votre compte GitHub → choisissez ce repo → `app.py`
4. Cliquez **Deploy** ✅

L'app sera accessible publiquement via `https://VOTRE_USER-onedrive-search-app-XXXX.streamlit.app`

---

## 🔐 Configurer l'application Azure AD

Vous devez créer une application dans le portail Azure pour autoriser l'accès à OneDrive.

### Étape 1 — Créer l'app dans Azure

1. Allez sur [portal.azure.com](https://portal.azure.com)
2. **Azure Active Directory** → **Inscriptions d'applications** → **Nouvelle inscription**
3. Nom : `OneDrive Search`
4. Types de comptes pris en charge : **Comptes dans n'importe quel annuaire + comptes Microsoft personnels**
5. URI de redirection : laissez vide (Device Flow ne nécessite pas de redirect)
6. Cliquez **Inscrire**

### Étape 2 — Récupérer les identifiants

Notez :
- **ID d'application (client)** → `Client ID`
- **ID de l'annuaire (tenant)** → `Tenant ID` (ou utilisez `common` pour les comptes perso)

### Étape 3 — Créer un secret client

1. **Certificats et secrets** → **Nouveau secret client**
2. Durée : 24 mois
3. Copiez la **valeur** du secret → `Client Secret`

### Étape 4 — Configurer les permissions API

1. **Autorisations d'API** → **Ajouter une autorisation** → **Microsoft Graph**
2. **Autorisations déléguées**, ajoutez :
   - `Files.Read.All`
   - `User.Read`
   - `offline_access`
3. Cliquez **Accorder le consentement administrateur** (si compte pro) OU l'utilisateur consentira lors de la première connexion

---

## 🔑 Sécuriser les secrets sur Streamlit Cloud

Ajoutez vos identifiants dans les **Secrets Streamlit** (Settings → Secrets) :

```toml
AZURE_CLIENT_ID = "votre-client-id"
AZURE_CLIENT_SECRET = "votre-client-secret"
AZURE_TENANT_ID = "common"
```

Puis dans `app.py`, remplacez les `st.text_input` par :
```python
import streamlit as st
client_id = st.secrets["AZURE_CLIENT_ID"]
```

---

## 📋 Fonctionnalités

| Fonctionnalité | Détail |
|---|---|
| 🔐 Auth OAuth2 | Device Flow Microsoft (pas besoin de redirect URI) |
| 📂 Crawl complet | Tous les fichiers OneDrive via Graph API delta |
| 🔄 Mise à jour delta | Re-indexation incrémentale (seulement les changements) |
| 📄 Extraction PDF | Via `pdfplumber` + fallback `PyPDF2` |
| 📝 Extraction Word | Via `python-docx` |
| 📊 Extraction Excel | Via `openpyxl` |
| 📑 Extraction PPTX | Via `python-pptx` |
| ⚡ FTS5 | Full-text search SQLite ultra-rapide |
| 🔍 3 modes | Nom seul / Contenu seul / Les deux |
| 🗂️ Filtre par type | PDF, Word, Excel, etc. |
| 🧵 Multi-thread | Téléchargement parallèle configurable |
| 📊 Statistiques | Dashboard avec répartition par type |

---

## 💡 Utilisation

1. Renseignez **Client ID**, **Secret**, **Tenant ID** dans la sidebar
2. Cliquez **Se connecter à OneDrive** → suivez le lien Device Flow dans votre navigateur
3. Allez dans l'onglet **Indexation** → **Indexation complète**
4. Une fois terminé, allez dans **Recherche** et tapez votre mot-clé

Le jeton d'authentification est mis en cache localement (`.token_cache.json`).  
L'index est stocké dans `onedrive_index.db` (SQLite).

---

## 🔒 Confidentialité

- Aucune donnée n'est envoyée à des serveurs tiers
- L'index SQLite reste en local (ou sur votre instance Streamlit Cloud privée)
- L'authentification utilise le protocole OAuth2 standard de Microsoft
