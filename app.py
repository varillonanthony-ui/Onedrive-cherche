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
    # Compact JSON (pas d indentation) = 2-3x plus petit = upload plus rapide
    content = json.dumps(data, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    size_kb = len(content) / 1024
    timeout = max(30, int(size_kb / 100) + 20)
    r = requests.put(
        f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}:/content",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        data=content, timeout=timeout)
    if r.status_code not in (200, 201):
        raise Exception(f"OneDrive erreur {r.status_code} : {r.text[:200]}")

def load_index(token):
    if "cache_index" not in st.session_state:
        with st.spinner("Chargement de l index..."):
            data = od_read(token, INDEX_FILE)
        st.session_state.cache_index = data if isinstance(data, list) else []
    return st.session_state.cache_index

def save_index(token, index):
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
    results = []
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

def search_local_index(token, keywords, file_types=None,
                       search_in_name=True, search_in_content=True, search_in_path=False):
    """
    Recherche multi-mots-cles avec logique AND stricte.
    Chaque mot-cle doit etre trouve dans au moins un champ COCHE.
    Regle : si un mot-cle ne matche QUE dans un champ decoche, le fichier est exclu.
    """
    index   = load_index(token)
    kw_list = [k.lower().strip() for k in keywords if k.strip()]
    if not kw_list:
        return []
    if not search_in_name and not search_in_content and not search_in_path:
        return []
    results = []

    for item in index:
        ext = item.get("ext", "")
        if file_types and ext not in file_types:
            continue

        # On recupere TOUJOURS les 3 champs pour le matching
        name_str    = str(item.get("name") or "").lower()
        path_str    = str(item.get("path") or "").lower()
        content_str = str(item.get("content") or "").lower()

        # Chaque mot-cle doit avoir au moins une correspondance dans un champ COCHE
        def kw_found_in_active(kw):
            if search_in_name    and kw in name_str:    return True
            if search_in_path    and kw in path_str:    return True
            if search_in_content and kw in content_str: return True
            return False

        if not all(kw_found_in_active(kw) for kw in kw_list):
            continue

        # Badges : le mot-cle est-il present dans ce champ (peu importe si coche ou non)
        match_name    = search_in_name    and any(kw in name_str    for kw in kw_list)
        match_path    = search_in_path    and any(kw in path_str    for kw in kw_list)
        match_content = search_in_content and any(kw in content_str for kw in kw_list)

        item_copy = dict(item)
        item_copy["match_name"]    = match_name
        item_copy["match_path"]    = match_path
        item_copy["match_content"] = match_content
        item_copy["keywords"]      = kw_list

        if match_content:
            content_full = str(item.get("content") or "")
            first_kw = next((kw for kw in kw_list if kw in content_full.lower()), kw_list[0])
            idx   = content_full.lower().find(first_kw)
            start = max(0, idx - 80)
            end   = min(len(content_full), idx + 120)
            item_copy["excerpt"] = "..." + content_full[start:end] + "..."
        elif match_path:
            # Extrait contextuel dans le chemin si pas de contenu
            first_kw = next((kw for kw in kw_list if kw in path_str), kw_list[0])
            idx   = path_str.find(first_kw)
            start = max(0, idx - 40)
            end   = min(len(path_str), idx + 80)
            item_copy["excerpt"] = "..." + path_str[start:end] + "..."

        item_copy["source"] = "local_index"
        results.append(item_copy)

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

def is_readable_text(text, min_ratio=0.80):
    """Retourne True si le texte est majoritairement lisible (pas du binaire corrompu)."""
    if not text:
        return False
    printable = sum(1 for c in text if c.isprintable() or c in '\n\r\t')
    return (printable / len(text)) >= min_ratio

def clean_text(text):
    """Supprime les caracteres non imprimables et normalise les espaces."""
    text = re.sub(r'[^\x09\x0a\x0d\x20-\x7e\x80-\xff]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Taille max a telecharger par type (evite de telecharger des gros fichiers inutilement)
MAX_DOWNLOAD = {
    "txt": 200_000, "csv": 200_000,
    "docx": 2_000_000, "doc": 2_000_000,
    "xlsx": 2_000_000, "xls": 2_000_000,
    "pptx": 2_000_000, "ppt": 2_000_000,
    # PDF : on n extrait plus le contenu (trop lent, trop peu fiable)
    # La recherche dans les PDFs passe par Graph Search (Microsoft l indexe nativement)
}
MAX_CONTENT_CHARS = 3000  # Limite du texte stocke dans l index

def extract_text_from_file(token, item_id, ext):
    """Extrait le texte. Les PDFs sont exclus (utiliser Graph Search a la place)."""
    if ext == "pdf":
        return ""  # PDF : Graph Search est plus fiable et plus rapide

    max_dl = MAX_DOWNLOAD.get(ext, 500_000)

    try:
        r = requests.get(
            f"{GRAPH_BASE}/me/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20, allow_redirects=True,
            stream=True)

        if r.status_code != 200:
            return ""

        # Lecture limitee (evite de telecharger des fichiers enormes)
        chunks = []
        total = 0
        for chunk in r.iter_content(chunk_size=32_768):
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_dl:
                break
        content_bytes = b"".join(chunks)

        if ext in ("txt", "csv"):
            try:
                text = content_bytes.decode("utf-8", errors="ignore")[:MAX_CONTENT_CHARS]
                return text if is_readable_text(text) else ""
            except Exception:
                return ""

        elif ext in ("docx", "doc"):
            try:
                import io, zipfile
                z = zipfile.ZipFile(io.BytesIO(content_bytes))
                xml = z.read("word/document.xml").decode("utf-8", errors="ignore")
                text = re.sub(r'<[^>]+>', ' ', xml)
                text = clean_text(text)[:MAX_CONTENT_CHARS]
                return text if is_readable_text(text) else ""
            except Exception:
                return ""

        elif ext in ("xlsx", "xls"):
            try:
                import io, zipfile
                z = zipfile.ZipFile(io.BytesIO(content_bytes))
                texts = []
                for name in z.namelist():
                    if name.startswith("xl/worksheets/") and name.endswith(".xml"):
                        xml  = z.read(name).decode("utf-8", errors="ignore")
                        text = re.sub(r'<[^>]+>', ' ', xml)
                        texts.append(clean_text(text))
                        if sum(len(t) for t in texts) > MAX_CONTENT_CHARS:
                            break  # Assez de texte
                result = " ".join(texts)[:MAX_CONTENT_CHARS]
                return result if is_readable_text(result) else ""
            except Exception:
                return ""

        elif ext in ("pptx", "ppt"):
            try:
                import io, zipfile
                z = zipfile.ZipFile(io.BytesIO(content_bytes))
                texts = []
                for name in sorted(z.namelist()):
                    if name.startswith("ppt/slides/slide") and name.endswith(".xml"):
                        xml  = z.read(name).decode("utf-8", errors="ignore")
                        text = re.sub(r'<[^>]+>', ' ', xml)
                        texts.append(clean_text(text))
                        if sum(len(t) for t in texts) > MAX_CONTENT_CHARS:
                            break
                result = " ".join(texts)[:MAX_CONTENT_CHARS]
                return result if is_readable_text(result) else ""
            except Exception:
                return ""

    except Exception:
        return ""


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

        st.markdown("**Afficher les résultats trouvés dans :**")
        col_c1, col_c2, col_c3 = st.columns(3)
        with col_c1:
            show_name_match    = st.checkbox("📛 Nom du fichier", value=True)
        with col_c2:
            show_content_match = st.checkbox("📄 Contenu du fichier", value=True)
        with col_c3:
            show_path_match    = st.checkbox("📂 Chemin du dossier", value=False)

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
                local_ids = {r["id"] for r in results}
                for g in graph:
                    if g["id"] not in local_ids:
                        results.append(g)

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

        if filter_path:
            results = [r for r in results
                       if filter_path.lower() in r.get("path","").lower()]

        # Filtre sur le type de correspondance
        def match_accepted(r):
            if r.get("source") == "graph_search":
                return True
            if r.get("match_name", False)    and show_name_match:    return True
            if r.get("match_content", False) and show_content_match: return True
            if r.get("match_path", False)    and show_path_match:    return True
            return False

        results = [r for r in results if match_accepted(r)]

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
                        if r.get("excerpt"):
                            st.markdown("**Extrait :**")
                            excerpt = r["excerpt"]
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
                        browse_search.lower() in str(f.get("name","")).lower() or
                        browse_search.lower() in str(f.get("path","")).lower()]
        if browse_type != "Tous":
            filtered = [f for f in filtered if f.get("type") == browse_type]

        sort_keys = {
            "Nom":    lambda x: str(x.get("name","")).lower(),
            "Date":   lambda x: str(x.get("modified","")),
            "Taille": lambda x: x.get("size",0),
            "Type":   lambda x: str(x.get("type",""))
        }
        filtered.sort(key=sort_keys[browse_sort],
                      reverse=(browse_sort in ["Date","Taille"]))

        st.write(f"**{len(filtered)} fichier(s)**")

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
            value=True)
    with col_o2:
        incremental = st.checkbox(
            "Indexation incrementale (ignorer les fichiers deja indexes)",
            value=True)

    autosave_every = st.slider(
        "Sauvegarde automatique toutes les N fichiers",
        min_value=10, max_value=200, value=50, step=10)

    st.caption(
        "💡 L index est sauvegarde automatiquement pendant l indexation. "
        "Vous pouvez fermer le navigateur a tout moment — "
        "relancez ensuite avec l indexation incrementale pour reprendre.")

    if st.button("🚀 Lancer l indexation", type="primary"):
        bar         = st.progress(0, text="Demarrage...")
        info        = st.empty()
        save_status = st.empty()
        t_start     = time.time()

        # Index existant : id -> date de modification (pour incremental fin)
        existing_map  = {f["id"]: f.get("modified","") for f in idx} if incremental else {}
        running_index = list(idx) if incremental else []
        new_index     = []
        skipped_count = [0]

        # Seuls ces types justifient un telechargement (PDF exclus : Graph Search)
        indexable = {"docx","doc","xlsx","xls","pptx","ppt","txt","csv"}

        def scan_with_filter(folder_id, current_path):
            try:
                endpoint  = (f"/me/drive/root/children"
                             if folder_id == "root"
                             else f"/me/drive/items/{folder_id}/children")
                next_link = (f"{GRAPH_BASE}{endpoint}?$top=200"
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
                            item_id  = item["id"]
                            modified = item.get("lastModifiedDateTime","")[:10]
                            # Incremental : sauter si deja indexe ET pas modifie
                            if incremental and item_id in existing_map:
                                if existing_map[item_id] == modified:
                                    skipped_count[0] += 1
                                    continue
                                # Modifie : retirer l ancienne entree avant re-indexation
                                running_index[:] = [f for f in running_index if f["id"] != item_id]
                            ext  = name.rsplit(".",1)[-1].lower() if "." in name else ""
                            size = item.get("size", 0)
                            # Extraire le contenu seulement si utile et fichier pas trop gros
                            content = ""
                            if extract_content and ext in indexable and size < 5*1024*1024:
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
                                "modified": modified,
                                "content":  content,
                                "indexed":  datetime.datetime.now().isoformat()
                            }
                            new_index.append(entry)
                            running_index.append(entry)
                            total_n = len(new_index)
                            info.markdown(
                                f"**{total_n} nouveaux** | ⏭️ {skipped_count[0]} ignores | "
                                f"`{current_path}/{name}`")
                            bar.progress(min(total_n/500, 1.0))
                            if total_n % autosave_every == 0:
                                try:
                                    save_index(token, running_index)
                                    save_status.success(
                                        f"💾 {total_n} fichiers sauvegardes ✓")
                                except Exception as e_save:
                                    save_status.warning(f"⚠️ Sauvegarde echouee : {e_save}")
                    next_link = data.get("@odata.nextLink")
            except Exception as e:
                info.warning(f"Erreur dossier {current_path} : {e}")

        scan_error = None
        try:
            scan_with_filter("root", "")
        except Exception as e:
            scan_error = e

        duration = int(time.time() - t_start)

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
            st.error(
                f"Impossible de sauvegarder sur OneDrive : {save_error}\n\n"
                f"{len(new_index)} fichiers ont ete indexes en memoire mais non sauvegardes.")
            if running_index:
                json_data = json.dumps(running_index, ensure_ascii=False, indent=2)
                st.download_button(
                    "⬇️ Telecharger l index localement (JSON)",
                    data=json_data,
                    file_name="file_index.json",
                    mime="application/json")

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
