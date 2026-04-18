"""
OneDrive Search Pro — Recherche intelligente dans vos fichiers OneDrive
=======================================================================
- Indexe tous vos fichiers OneDrive (PDF, Word, Excel, PowerPoint, etc.)
- Recherche par mot-cle dans le nom du fichier ET dans le contenu
- Utilise Microsoft Graph Search API (pas besoin de telecharger les fichiers)
- Index stocke sur OneDrive pour acces depuis n importe quel appareil

Prerequis : pip install requests streamlit
Secrets Streamlit :
  AZURE_CLIENT_ID = "..."
  AZURE_TENANT_ID = "..."
  APP_PASSWORD    = "..."
"""

import streamlit as st
import requests
import json
import re
import time
import datetime

GRAPH_BASE      = "https://graph.microsoft.com/v1.0"
ONEDRIVE_FOLDER = "OneDriveSearch"
INDEX_FILE      = "file_index.json"

# Types de fichiers supportes
FILE_TYPES = {
    "pdf":  {"icon": "📄", "label": "PDF"},
    "docx": {"icon": "📝", "label": "Word"},
    "doc":  {"icon": "📝", "label": "Word"},
    "xlsx": {"icon": "📊", "label": "Excel"},
    "xls":  {"icon": "📊", "label": "Excel"},
    "pptx": {"icon": "📑", "label": "PowerPoint"},
    "ppt":  {"icon": "📑", "label": "PowerPoint"},
    "txt":  {"icon": "📃", "label": "Texte"},
    "csv":  {"icon": "📊", "label": "CSV"},
    "msg":  {"icon": "✉️",  "label": "Email"},
    "eml":  {"icon": "✉️",  "label": "Email"},
    "png":  {"icon": "🖼️",  "label": "Image"},
    "jpg":  {"icon": "🖼️",  "label": "Image"},
    "jpeg": {"icon": "🖼️",  "label": "Image"},
    "zip":  {"icon": "🗜️",  "label": "Archive"},
    "mp4":  {"icon": "🎬",  "label": "Video"},
}


# ──────────────────────────────────────────
# GRAPH API
# ──────────────────────────────────────────
def graph_get(token, endpoint):
    r = requests.get(f"{GRAPH_BASE}{endpoint}",
                     headers={"Authorization": f"Bearer {token}"}, timeout=20)
    r.raise_for_status()
    return r.json()

def graph_post(token, endpoint, body):
    r = requests.post(f"{GRAPH_BASE}{endpoint}",
                      headers={"Authorization": f"Bearer {token}",
                               "Content-Type": "application/json"},
                      json=body, timeout=20)
    r.raise_for_status()
    return r.json()


# ──────────────────────────────────────────
# ONEDRIVE — STOCKAGE DE L INDEX
# ──────────────────────────────────────────
def od_read(token, filename):
    try:
        r = requests.get(
            f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}:/content",
            headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def od_write(token, filename, data):
    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    size_kb = len(content) / 1024
    # Timeout adaptatif : 30s pour petit, 120s pour gros fichiers
    timeout = max(60, int(size_kb / 50) + 30)
    r = requests.put(
        f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}:/content",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        data=content, timeout=timeout)
    if r.status_code not in (200, 201):
        raise Exception(f"OneDrive erreur {r.status_code} : {r.text[:200]}")

def load_index(token):
    if "cache_index" not in st.session_state:
        data = od_read(token, INDEX_FILE)
        st.session_state.cache_index = data if isinstance(data, list) else []
    return st.session_state.cache_index

def save_index(token, index):
    """Sauvegarde l index sur OneDrive avec retry en cas d echec."""
    for attempt in range(3):
        try:
            od_write(token, INDEX_FILE, index)
            st.session_state.cache_index = index
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                raise Exception(f"Sauvegarde OneDrive echouee apres 3 tentatives : {e}")


# ──────────────────────────────────────────
# RECHERCHE GRAPH API
# ──────────────────────────────────────────
def search_graph(token, query, file_types=None):
    """Recherche dans OneDrive via Microsoft Graph Search API.
    Cherche dans le nom du fichier ET dans le contenu des fichiers."""
    results = []

    # Construire la requete KQL
    kql = query
    if file_types:
        ext_filter = " OR ".join([f"filetype:{ext}" for ext in file_types])
        kql = f"({query}) AND ({ext_filter})"

    try:
        payload = {
            "requests": [{
                "entityTypes": ["driveItem"],
                "query": {"queryString": kql},
                "from": 0,
                "size": 50,
                "fields": ["id", "name", "webUrl", "lastModifiedDateTime",
                           "size", "parentReference", "file"]
            }]
        }
        resp = requests.post(
            f"{GRAPH_BASE}/search/query",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            json=payload, timeout=20)

        if resp.status_code == 200:
            data = resp.json()
            hits = (data.get("value", [{}])[0]
                       .get("hitsContainers", [{}])[0]
                       .get("hits", []))
            for hit in hits:
                resource = hit.get("resource", {})
                name = resource.get("name", "")
                ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                results.append({
                    "id":       resource.get("id", ""),
                    "name":     name,
                    "ext":      ext,
                    "icon":     FILE_TYPES.get(ext, {}).get("icon", "📁"),
                    "type":     FILE_TYPES.get(ext, {}).get("label", "Fichier"),
                    "url":      resource.get("webUrl", ""),
                    "modified": resource.get("lastModifiedDateTime", "")[:10],
                    "size":     resource.get("size", 0),
                    "path":     resource.get("parentReference", {}).get("path", "").replace("/drive/root:", ""),
                    "summary":  hit.get("summary", ""),
                    "source":   "graph_search"
                })
    except Exception as e:
        st.warning(f"Recherche Graph : {e}")

    return results

def search_local_index(token, query, file_types=None):
    """Recherche dans l index local (nom + contenu extrait)."""
    index   = load_index(token)
    query_l = query.lower()
    results = []

    for item in index:
        ext = item.get("ext", "")
        if file_types and ext not in file_types:
            continue

        # Chercher dans le nom
        name_match = query_l in item.get("name", "").lower()
        # Chercher dans le chemin
        path_match = query_l in item.get("path", "").lower()
        # Chercher dans le contenu extrait
        content_match = query_l in item.get("content", "").lower()

        if name_match or path_match or content_match:
            item_copy = dict(item)
            item_copy["match_name"]    = name_match
            item_copy["match_path"]    = path_match
            item_copy["match_content"] = content_match
            # Extrait contextuel
            if content_match:
                content = item.get("content", "")
                idx     = content.lower().find(query_l)
                start   = max(0, idx - 80)
                end     = min(len(content), idx + 120)
                item_copy["excerpt"] = "..." + content[start:end] + "..."
            item_copy["source"] = "local_index"
            results.append(item_copy)

    # Trier : nom > contenu > chemin
    results.sort(key=lambda x: (not x["match_name"], not x["match_content"], not x["match_path"]))
    return results


# ──────────────────────────────────────────
# INDEXATION
# ──────────────────────────────────────────
def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} o"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f} Ko"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/1024/1024:.1f} Mo"
    return f"{size_bytes/1024/1024/1024:.1f} Go"

def extract_text_from_file(token, item_id, ext):
    """Extrait le texte d un fichier via Graph API content endpoint."""
    try:
        # Telecharger le contenu
        r = requests.get(
            f"{GRAPH_BASE}/me/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30, allow_redirects=True)

        if r.status_code != 200:
            return ""

        content_bytes = r.content

        # Extraction selon le type
        if ext == "txt" or ext == "csv":
            try:
                return content_bytes.decode("utf-8", errors="ignore")[:5000]
            except Exception:
                return ""

        elif ext in ("docx",):
            try:
                import io, zipfile
                z = zipfile.ZipFile(io.BytesIO(content_bytes))
                xml = z.read("word/document.xml").decode("utf-8", errors="ignore")
                text = re.sub(r'<[^>]+>', ' ', xml)
                text = re.sub(r'\s+', ' ', text).strip()
                return text[:5000]
            except Exception:
                return ""

        elif ext in ("xlsx",):
            try:
                import io, zipfile
                z = zipfile.ZipFile(io.BytesIO(content_bytes))
                texts = []
                for name in z.namelist():
                    if name.startswith("xl/worksheets/") and name.endswith(".xml"):
                        xml  = z.read(name).decode("utf-8", errors="ignore")
                        text = re.sub(r'<[^>]+>', ' ', xml)
                        texts.append(re.sub(r'\s+', ' ', text).strip())
                return " ".join(texts)[:5000]
            except Exception:
                return ""

        elif ext in ("pptx",):
            try:
                import io, zipfile
                z = zipfile.ZipFile(io.BytesIO(content_bytes))
                texts = []
                for name in z.namelist():
                    if name.startswith("ppt/slides/slide") and name.endswith(".xml"):
                        xml  = z.read(name).decode("utf-8", errors="ignore")
                        text = re.sub(r'<[^>]+>', ' ', xml)
                        texts.append(re.sub(r'\s+', ' ', text).strip())
                return " ".join(texts)[:5000]
            except Exception:
                return ""

        elif ext == "pdf":
            try:
                # Extraction PDF basique par recherche de texte brut
                raw = content_bytes.decode("latin-1", errors="ignore")
                # Extraire les streams de texte PDF
                texts = re.findall(r'BT\s*(.*?)\s*ET', raw, re.DOTALL)
                extracted = []
                for t in texts:
                    parts = re.findall(r'\((.*?)\)', t)
                    extracted.extend(parts)
                text = " ".join(extracted)
                text = re.sub(r'\s+', ' ', text).strip()
                return text[:5000] if text else ""
            except Exception:
                return ""

    except Exception:
        return ""

def index_all_files(token, progress_bar, status_text,
                    extract_content=True, folder_path="/"):
    """Indexe tous les fichiers OneDrive avec extraction de contenu."""
    index   = []
    total   = 0
    skipped = 0

    # Extensions indexables
    indexable = {"pdf","docx","doc","xlsx","xls","pptx","ppt","txt","csv","msg","eml"}

    def scan_folder(folder_id, current_path):
        nonlocal total, skipped
        try:
            endpoint = (f"/me/drive/root/children"
                        if folder_id == "root"
                        else f"/me/drive/items/{folder_id}/children")
            next_link = f"{GRAPH_BASE}{endpoint}?$top=100&$select=id,name,size,file,folder,lastModifiedDateTime,webUrl,parentReference"

            while next_link:
                r = requests.get(next_link,
                                  headers={"Authorization": f"Bearer {token}"},
                                  timeout=20)
                r.raise_for_status()
                data  = r.json()
                items = data.get("value", [])

                for item in items:
                    name = item.get("name", "")

                    if item.get("folder"):
                        # Sous-dossier : recursion
                        sub_path = f"{current_path}/{name}"
                        scan_folder(item["id"], sub_path)
                    elif item.get("file"):
                        ext      = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                        size     = item.get("size", 0)
                        modified = item.get("lastModifiedDateTime", "")[:10]
                        web_url  = item.get("webUrl", "")
                        item_id  = item["id"]

                        # Extraction de contenu
                        content = ""
                        if extract_content and ext in indexable and size < 10 * 1024 * 1024:
                            content = extract_text_from_file(token, item_id, ext)
                        elif size >= 10 * 1024 * 1024:
                            skipped += 1

                        index.append({
                            "id":       item_id,
                            "name":     name,
                            "ext":      ext,
                            "icon":     FILE_TYPES.get(ext, {}).get("icon", "📁"),
                            "type":     FILE_TYPES.get(ext, {}).get("label", "Fichier"),
                            "path":     current_path,
                            "url":      web_url,
                            "size":     size,
                            "modified": modified,
                            "content":  content,
                            "indexed":  datetime.datetime.now().isoformat()
                        })
                        total += 1
                        status_text.markdown(
                            f"**{total} fichiers indexes...** `{current_path}/{name}`")
                        progress_bar.progress(min(total / 1000, 1.0))

                next_link = data.get("@odata.nextLink")
        except Exception as e:
            status_text.warning(f"Erreur dossier {current_path} : {e}")

    scan_folder("root", "")
    return index, total, skipped


# ──────────────────────────────────────────
# CONNEXION
# ──────────────────────────────────────────
def start_device_flow():
    r = requests.post(
        f"https://login.microsoftonline.com/{st.secrets['AZURE_TENANT_ID']}/oauth2/v2.0/devicecode",
        data={"client_id": st.secrets["AZURE_CLIENT_ID"],
              "scope": "Files.Read.All Files.ReadWrite offline_access"},
        timeout=15)
    r.raise_for_status()
    return r.json()

def poll_token(device_code):
    r = requests.post(
        f"https://login.microsoftonline.com/{st.secrets['AZURE_TENANT_ID']}/oauth2/v2.0/token",
        data={"client_id": st.secrets["AZURE_CLIENT_ID"],
              "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
              "device_code": device_code},
        timeout=15)
    data = r.json()
    return data.get("access_token"), data.get("refresh_token")


# ══════════════════════════════════════════
# INTERFACE
# ══════════════════════════════════════════
st.set_page_config(page_title="OneDrive Search Pro", page_icon="🔍", layout="wide")

# ── MOT DE PASSE ──
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.title("🔍 OneDrive Search Pro")
    _, col, _ = st.columns([1,2,1])
    with col:
        st.markdown("### Acces securise")
        pwd = st.text_input("Mot de passe", type="password")
        if st.button("Se connecter", type="primary", use_container_width=True):
            if pwd == st.secrets.get("APP_PASSWORD",""):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Mot de passe incorrect.")
    return False

if not check_password():
    st.stop()

# ── CONNEXION OFFICE 365 ──
for k, v in [("token", None), ("device_flow", None)]:
    if k not in st.session_state:
        st.session_state[k] = v

if not st.session_state.token:
    st.title("🔍 OneDrive Search Pro")
    st.subheader("Connexion OneDrive")

    if st.button("Se connecter a OneDrive", type="primary"):
        with st.spinner("Initialisation..."):
            try:
                st.session_state.device_flow = start_device_flow()
            except Exception as e:
                st.error(f"Erreur : {e}")

    if st.session_state.device_flow:
        flow = st.session_state.device_flow
        m    = re.search(r'enter the code ([A-Z0-9]+)', flow.get("message",""))
        code = m.group(1) if m else ""
        col1, col2 = st.columns([2,1])
        with col1:
            st.markdown("**1.** Ouvrez : [https://microsoft.com/devicelogin](https://microsoft.com/devicelogin)")
            st.markdown("**2.** Entrez le code ci-contre")
            st.markdown("**3.** Connectez-vous avec votre compte Microsoft")
        with col2:
            st.info(f"Code :\n# {code}")
        if st.button("J ai valide le code ✓", type="primary"):
            with st.spinner("Verification..."):
                for _ in range(30):
                    tok, _ = poll_token(flow["device_code"])
                    if tok:
                        st.session_state.token = tok
                        st.rerun()
                        break
                    time.sleep(3)
                else:
                    st.error("Timeout. Relancez.")
    st.stop()

token = st.session_state.token

# ── SIDEBAR ──
with st.sidebar:
    st.title("🔍 OneDrive Search Pro")
    idx = load_index(token)
    st.metric("Fichiers indexes", len(idx))
    if idx:
        types = {}
        for f in idx:
            t = f.get("type","Autre")
            types[t] = types.get(t,0) + 1
        for t, n in sorted(types.items(), key=lambda x: -x[1])[:6]:
            st.caption(f"{n} {t}")
    st.divider()
    if st.button("Vider le cache", use_container_width=True):
        st.session_state.pop("cache_index", None)
        st.rerun()
    if st.button("Deconnecter", use_container_width=True):
        for k in ["token","device_flow","authenticated","cache_index"]:
            st.session_state.pop(k, None)
        st.rerun()

# ── ONGLETS ──
tab1, tab2, tab3 = st.tabs(["🔍 Recherche", "📂 Parcourir", "🗂️ Indexation"])


# ════════════════════════════════════════
# ONGLET 1 — RECHERCHE
# ════════════════════════════════════════
with tab1:
    st.subheader("Recherche dans vos fichiers OneDrive")

    col_q, col_mode = st.columns([3,1])
    with col_q:
        query = st.text_input("",
                              placeholder="Rechercher un mot-cle dans le nom ou le contenu des fichiers...",
                              label_visibility="collapsed")
    with col_mode:
        search_mode = st.selectbox("Mode", ["Index local", "Graph Search", "Les deux"],
                                   label_visibility="collapsed")

    # Filtres
    with st.expander("🎛️ Filtres"):
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            filter_types = st.multiselect("Types de fichiers",
                options=list(set(FILE_TYPES.keys())),
                default=[],
                placeholder="Tous les types")
        with col_f2:
            filter_date = st.selectbox("Modifie depuis",
                ["Toujours","Aujourd hui","Cette semaine","Ce mois","Cette annee"])
        with col_f3:
            filter_path = st.text_input("Dans le dossier", placeholder="ex: Documents/Projets")

    if query and st.button("Rechercher", type="primary", key="search_btn") or (
            query and st.session_state.get("last_query") == query):
        st.session_state["last_query"] = query

        results = []

        with st.spinner("Recherche en cours..."):
            if search_mode in ("Index local", "Les deux"):
                local = search_local_index(token, query,
                                           file_types=filter_types if filter_types else None)
                results.extend(local)

            if search_mode in ("Graph Search", "Les deux"):
                graph = search_graph(token, query,
                                     file_types=filter_types if filter_types else None)
                # Deduplication
                local_ids = {r["id"] for r in results}
                for g in graph:
                    if g["id"] not in local_ids:
                        results.append(g)

        # Filtrer par date
        if filter_date != "Toujours":
            now = datetime.datetime.now()
            cutoffs = {
                "Aujourd hui": now - datetime.timedelta(days=1),
                "Cette semaine": now - datetime.timedelta(days=7),
                "Ce mois": now - datetime.timedelta(days=30),
                "Cette annee": now - datetime.timedelta(days=365),
            }
            cutoff = cutoffs.get(filter_date)
            if cutoff:
                results = [r for r in results
                           if r.get("modified","") >= cutoff.strftime("%Y-%m-%d")]

        # Filtrer par chemin
        if filter_path:
            results = [r for r in results
                       if filter_path.lower() in r.get("path","").lower()]

        if not results:
            st.info(f"Aucun resultat pour « {query} »")
        else:
            st.success(f"**{len(results)} fichier(s) trouve(s)** pour « {query} »")

            for r in results:
                icon = r.get("icon","📁")
                name = r.get("name","")
                path = r.get("path","")
                url  = r.get("url","")
                mod  = r.get("modified","")
                size = format_size(r.get("size",0))
                typ  = r.get("type","Fichier")

                # Badges sources
                badges = []
                if r.get("match_name"):    badges.append("🏷️ Nom")
                if r.get("match_content"): badges.append("📄 Contenu")
                if r.get("match_path"):    badges.append("📂 Chemin")
                if r.get("source") == "graph_search": badges.append("🔎 Graph")
                badge_str = "  ".join(badges)

                with st.expander(f"{icon} **{name}** — {size} — {mod}  {badge_str}"):
                    col_i, col_a = st.columns([3,1])
                    with col_i:
                        st.markdown(f"**Type :** {typ}")
                        st.markdown(f"**Chemin :** `{path}/{name}`")
                        st.markdown(f"**Modifie le :** {mod} | **Taille :** {size}")
                        # Extrait contextuel
                        if r.get("excerpt"):
                            st.markdown("**Extrait :**")
                            excerpt = r["excerpt"]
                            # Surligner le mot-cle
                            highlighted = re.sub(
                                f"({re.escape(query)})",
                                r"**\1**",
                                excerpt, flags=re.IGNORECASE)
                            st.markdown(f"> {highlighted}")
                        elif r.get("summary"):
                            st.markdown("**Extrait :**")
                            st.markdown(f"> {r['summary']}")
                    with col_a:
                        if url:
                            st.link_button("📂 Ouvrir", url, use_container_width=True)


# ════════════════════════════════════════
# ONGLET 2 — PARCOURIR
# ════════════════════════════════════════
with tab2:
    st.subheader("Parcourir les fichiers indexes")

    idx = load_index(token)
    if not idx:
        st.info("Aucun fichier indexe. Lancez l indexation depuis l onglet Indexation.")
    else:
        # Filtres navigation
        col_b1, col_b2, col_b3 = st.columns([2,1,1])
        with col_b1:
            browse_search = st.text_input("Filtrer", placeholder="nom, dossier...")
        with col_b2:
            browse_type = st.selectbox("Type", ["Tous"] + sorted(set(
                f.get("type","Autre") for f in idx)))
        with col_b3:
            browse_sort = st.selectbox("Trier par", ["Nom", "Date", "Taille", "Type"])

        filtered = idx
        if browse_search:
            filtered = [f for f in filtered if
                        browse_search.lower() in f.get("name","").lower() or
                        browse_search.lower() in f.get("path","").lower()]
        if browse_type != "Tous":
            filtered = [f for f in filtered if f.get("type") == browse_type]

        sort_keys = {
            "Nom":   lambda x: x.get("name","").lower(),
            "Date":  lambda x: x.get("modified",""),
            "Taille":lambda x: x.get("size",0),
            "Type":  lambda x: x.get("type","")
        }
        filtered.sort(key=sort_keys[browse_sort],
                      reverse=(browse_sort in ["Date","Taille"]))

        st.write(f"**{len(filtered)} fichier(s)**")

        # Regrouper par dossier
        folders = {}
        for f in filtered:
            path = f.get("path","/") or "/"
            folders.setdefault(path, []).append(f)

        for folder_path in sorted(folders.keys()):
            files = folders[folder_path]
            with st.expander(f"📁 `{folder_path or '/'}` — {len(files)} fichier(s)"):
                for f in files:
                    col_n, col_s, col_d, col_a = st.columns([4,1,1,1])
                    with col_n:
                        st.markdown(f"{f.get('icon','📁')} **{f.get('name','')}**")
                    with col_s:
                        st.caption(format_size(f.get("size",0)))
                    with col_d:
                        st.caption(f.get("modified",""))
                    with col_a:
                        if f.get("url"):
                            st.link_button("Ouvrir", f["url"], use_container_width=True)


# ════════════════════════════════════════
# ONGLET 3 — INDEXATION
# ════════════════════════════════════════
with tab3:
    st.subheader("Indexation de votre OneDrive")

    idx = load_index(token)
    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        st.metric("Fichiers indexes", len(idx))
    with col_m2:
        with_content = sum(1 for f in idx if f.get("content"))
        st.metric("Avec contenu extrait", with_content)
    with col_m3:
        if idx:
            last = max((f.get("indexed","") for f in idx), default="")
            st.metric("Derniere indexation", last[:10] if last else "Jamais")

    st.divider()

    st.info(
        "L indexation parcourt tout votre OneDrive, extrait le texte des fichiers "
        "(PDF, Word, Excel, PowerPoint, TXT) et sauvegarde un index sur OneDrive. "
        "Les fichiers de plus de 10 Mo sont references sans extraction de contenu."
    )

    col_o1, col_o2 = st.columns(2)
    with col_o1:
        extract_content = st.checkbox(
            "Extraire le contenu des fichiers (recherche dans le texte)",
            value=True,
            help="Desactivez pour une indexation rapide (nom et chemin uniquement)")
    with col_o2:
        # Incrementale par defaut (toujours True si un index existe)
        incremental = st.checkbox(
            "Indexation incrementale (ignorer les fichiers deja indexes)",
            value=True)
    # Sauvegarde automatique toutes les N fichiers
    autosave_every = st.slider(
        "Sauvegarde automatique toutes les N fichiers",
        min_value=10, max_value=200, value=50, step=10,
        help="L index est sauvegarde sur OneDrive regulierement. "
             "Fermez le navigateur a tout moment — relancez avec l incrementale pour continuer.")

    st.caption(
        "💡 L index est sauvegarde automatiquement pendant l indexation. "
        "Vous pouvez fermer le navigateur a tout moment — "
        "relancez ensuite avec l indexation incrementale pour reprendre.")

    if st.button("🚀 Lancer l indexation", type="primary"):
        bar         = st.progress(0, text="Demarrage...")
        info        = st.empty()
        save_status = st.empty()
        t_start     = time.time()

        existing_ids  = {f["id"] for f in idx} if incremental else set()
        running_index = list(idx) if incremental else []
        new_index     = []

        indexable = {"pdf","docx","doc","xlsx","xls","pptx","ppt","txt","csv"}

        def scan_with_filter(folder_id, current_path):
            try:
                endpoint  = (f"/me/drive/root/children"
                             if folder_id == "root"
                             else f"/me/drive/items/{folder_id}/children")
                next_link = (f"{GRAPH_BASE}{endpoint}?$top=100"
                             "&$select=id,name,size,file,folder,"
                             "lastModifiedDateTime,webUrl,parentReference")

                while next_link:
                    r = requests.get(next_link,
                                     headers={"Authorization": f"Bearer {token}"},
                                     timeout=20)
                    r.raise_for_status()
                    data  = r.json()
                    items = data.get("value", [])

                    for item in items:
                        name = item.get("name","")
                        if item.get("folder"):
                            scan_with_filter(item["id"], f"{current_path}/{name}")
                        elif item.get("file"):
                            item_id = item["id"]
                            if incremental and item_id in existing_ids:
                                continue
                            ext     = name.rsplit(".",1)[-1].lower() if "." in name else ""
                            size    = item.get("size", 0)
                            content = ""
                            if extract_content and ext in indexable and size < 10*1024*1024:
                                content = extract_text_from_file(token, item_id, ext)
                            entry = {
                                "id":       item_id,
                                "name":     name,
                                "ext":      ext,
                                "icon":     FILE_TYPES.get(ext,{}).get("icon","📁"),
                                "type":     FILE_TYPES.get(ext,{}).get("label","Fichier"),
                                "path":     current_path,
                                "url":      item.get("webUrl",""),
                                "size":     size,
                                "modified": item.get("lastModifiedDateTime","")[:10],
                                "content":  content,
                                "indexed":  datetime.datetime.now().isoformat()
                            }
                            new_index.append(entry)
                            running_index.append(entry)
                            total_n = len(new_index)
                            info.markdown(
                                f"**{total_n} nouveaux fichiers indexes...** "
                                f"`{current_path}/{name}`")
                            bar.progress(min(total_n/500, 1.0))
                            # Sauvegarde automatique
                            if total_n % autosave_every == 0:
                                try:
                                    save_index(token, running_index)
                                    save_status.success(
                                        f"💾 Sauvegarde automatique — "
                                        f"{total_n} fichiers sauvegardes sur OneDrive ✓")
                                except Exception as e_save:
                                    save_status.warning(
                                        f"⚠️ Sauvegarde auto echouee : {e_save}")
                    next_link = data.get("@odata.nextLink")
            except Exception as e:
                info.warning(f"Erreur dossier {current_path} : {e}")

        # Sauvegarde finale — separee du scan pour afficher l erreur clairement
        scan_error = None
        try:
            scan_with_filter("root", "")
        except Exception as e:
            scan_error = e

        duration = int(time.time() - t_start)

        # Toujours tenter de sauvegarder, meme si le scan a echoue
        saved_ok = False
        save_error = None
        if running_index:
            try:
                save_index(token, running_index)
                saved_ok = True
            except Exception as e:
                save_error = e

        bar.progress(1.0, text="Termine !")
        save_status.empty()

        if saved_ok:
            st.success(
                f"Indexation terminee en {duration}s — "
                f"**{len(new_index)} nouveaux fichiers** indexes "
                f"(total : {len(running_index)})")
            if scan_error:
                st.warning(f"Attention — scan interrompu : {scan_error}")
            st.balloons()
        else:
            # Sauvegarde echouee : sauvegarder localement en JSON telechargeable
            st.error(
                f"Impossible de sauvegarder sur OneDrive : {save_error}\n\n"
                f"{len(new_index)} fichiers ont ete indexes en memoire mais non sauvegardes.")
            if running_index:
                json_data = json.dumps(running_index, ensure_ascii=False, indent=2)
                st.download_button(
                    "⬇️ Telecharger l index localement (JSON)",
                    data=json_data,
                    file_name="email_index.json",
                    mime="application/json",
                    help="Sauvegardez ce fichier puis re-uploadez-le sur OneDrive/EmailSearch/"
                )


    st.divider()
    if idx and st.button("🗑️ Supprimer l index", type="secondary"):
        if st.session_state.get("confirm_delete"):
            save_index(token, [])
            st.session_state.pop("confirm_delete", None)
            st.success("Index supprime.")
            st.rerun()
        else:
            st.session_state["confirm_delete"] = True
            st.warning("Cliquez a nouveau pour confirmer la suppression.")
