"""
OneDrive Search Pro — Recherche intelligente dans vos fichiers OneDrive
"""
import streamlit as st
import requests
import json
import re
import time
import datetime
import os

GRAPH_BASE      = "https://graph.microsoft.com/v1.0"
ONEDRIVE_FOLDER = "OneDriveSearch"
INDEX_FILE      = "file_index.json"
LOCAL_CACHE     = "/tmp/onedrive_index_cache.json"

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

MAX_DOWNLOAD = {
    "txt": 200_000, "csv": 200_000,
    "docx": 2_000_000, "doc": 2_000_000,
    "xlsx": 2_000_000, "xls": 2_000_000,
    "pptx": 2_000_000, "ppt": 2_000_000,
}
MAX_CONTENT_CHARS = 3000


# ──────────────────────────────────────────
# UTILITAIRES TEXTE
# ──────────────────────────────────────────
def is_readable_text(text, min_ratio=0.80):
    if not text:
        return False
    printable = sum(1 for c in text if c.isprintable() or c in '\n\r\t')
    return (printable / len(text)) >= min_ratio

def clean_text(text):
    text = re.sub(r'[^\x09\x0a\x0d\x20-\x7e\x80-\xff]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} o"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f} Ko"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/1024/1024:.1f} Mo"
    return f"{size_bytes/1024/1024/1024:.1f} Go"


# ──────────────────────────────────────────
# STOCKAGE INDEX
# ──────────────────────────────────────────
def _read_local_cache():
    try:
        if os.path.exists(LOCAL_CACHE):
            with open(LOCAL_CACHE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return None

def _write_local_cache(data):
    try:
        with open(LOCAL_CACHE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        pass

def od_read(token, filename):
    try:
        r = requests.get(
            f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}:/content",
            headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def od_write(token, filename, data):
    content = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    size_kb = len(content) / 1024
    timeout = max(30, int(size_kb / 100) + 20)
    r = requests.put(
        f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}:/content",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=content, timeout=timeout)
    if r.status_code not in (200, 201):
        raise Exception(f"OneDrive erreur {r.status_code} : {r.text[:200]}")

def load_index(token):
    """Priorité : mémoire > /tmp > OneDrive."""
    if "cache_index" in st.session_state:
        return st.session_state.cache_index
    local = _read_local_cache()
    if local is not None:
        st.session_state.cache_index = local
        return local
    with st.spinner("Chargement de l'index depuis OneDrive..."):
        data = od_read(token, INDEX_FILE)
    result = data if isinstance(data, list) else []
    st.session_state.cache_index = result
    _write_local_cache(result)
    return result

def save_index(token, index):
    st.session_state.cache_index = index
    _write_local_cache(index)
    for attempt in range(3):
        try:
            od_write(token, INDEX_FILE, index)
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                raise Exception(f"Sauvegarde échouée après 3 tentatives : {e}")


# ──────────────────────────────────────────
# RECHERCHE
# ──────────────────────────────────────────
def search_graph(token, query, file_types=None):
    results = []
    kql = query
    if file_types:
        ext_filter = " OR ".join([f"filetype:{ext}" for ext in file_types])
        kql = f"({query}) AND ({ext_filter})"
    try:
        payload = {"requests": [{"entityTypes": ["driveItem"],
            "query": {"queryString": kql}, "from": 0, "size": 50,
            "fields": ["id","name","webUrl","lastModifiedDateTime","size","parentReference","file"]}]}
        resp = requests.post(f"{GRAPH_BASE}/search/query",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            hits = (data.get("value",[{}])[0].get("hitsContainers",[{}])[0].get("hits",[]))
            for hit in hits:
                resource = hit.get("resource", {})
                name = resource.get("name", "")
                ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                results.append({
                    "id":       resource.get("id", ""),
                    "name":     name, "ext": ext,
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
    index   = load_index(token)
    kw_list = [k.lower().strip() for k in keywords if k.strip()]
    if not kw_list or not (search_in_name or search_in_content or search_in_path):
        return []
    results = []
    for item in index:
        ext = item.get("ext", "")
        if file_types and ext not in file_types:
            continue
        name_str    = str(item.get("name") or "").lower()
        path_str    = str(item.get("path") or "").lower()
        content_str = str(item.get("content") or "").lower()

        def kw_found_in_active(kw):
            if search_in_name    and kw in name_str:    return True
            if search_in_path    and kw in path_str:    return True
            if search_in_content and kw in content_str: return True
            return False

        if not all(kw_found_in_active(kw) for kw in kw_list):
            continue

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
# EXTRACTION CONTENU
# ──────────────────────────────────────────
def extract_text_from_file(token, item_id, ext):
    if ext == "pdf":
        return ""
    max_dl = MAX_DOWNLOAD.get(ext, 500_000)
    try:
        r = requests.get(f"{GRAPH_BASE}/me/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20, allow_redirects=True, stream=True)
        if r.status_code != 200:
            return ""
        chunks, total = [], 0
        for chunk in r.iter_content(chunk_size=32_768):
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_dl:
                break
        content_bytes = b"".join(chunks)

        if ext in ("txt", "csv"):
            text = content_bytes.decode("utf-8", errors="ignore")[:MAX_CONTENT_CHARS]
            return text if is_readable_text(text) else ""
        elif ext in ("docx", "doc"):
            import io, zipfile
            z = zipfile.ZipFile(io.BytesIO(content_bytes))
            xml = z.read("word/document.xml").decode("utf-8", errors="ignore")
            text = clean_text(re.sub(r'<[^>]+>', ' ', xml))[:MAX_CONTENT_CHARS]
            return text if is_readable_text(text) else ""
        elif ext in ("xlsx", "xls"):
            import io, zipfile
            z = zipfile.ZipFile(io.BytesIO(content_bytes))
            texts = []
            for name in z.namelist():
                if name.startswith("xl/worksheets/") and name.endswith(".xml"):
                    xml = z.read(name).decode("utf-8", errors="ignore")
                    texts.append(clean_text(re.sub(r'<[^>]+>', ' ', xml)))
                    if sum(len(t) for t in texts) > MAX_CONTENT_CHARS:
                        break
            result = " ".join(texts)[:MAX_CONTENT_CHARS]
            return result if is_readable_text(result) else ""
        elif ext in ("pptx", "ppt"):
            import io, zipfile
            z = zipfile.ZipFile(io.BytesIO(content_bytes))
            texts = []
            for name in sorted(z.namelist()):
                if name.startswith("ppt/slides/slide") and name.endswith(".xml"):
                    xml = z.read(name).decode("utf-8", errors="ignore")
                    texts.append(clean_text(re.sub(r'<[^>]+>', ' ', xml)))
                    if sum(len(t) for t in texts) > MAX_CONTENT_CHARS:
                        break
            result = " ".join(texts)[:MAX_CONTENT_CHARS]
            return result if is_readable_text(result) else ""
    except Exception:
        return ""
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

def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.title("🔍 OneDrive Search Pro")
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("### Accès sécurisé")
        pwd = st.text_input("Mot de passe", type="password")
        if st.button("Se connecter", type="primary", use_container_width=True):
            if pwd == st.secrets.get("APP_PASSWORD", ""):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Mot de passe incorrect.")
    return False

if not check_password():
    st.stop()

for k, v in [("token", None), ("device_flow", None)]:
    if k not in st.session_state:
        st.session_state[k] = v

if not st.session_state.token:
    st.title("🔍 OneDrive Search Pro")
    st.subheader("Connexion OneDrive")
    if st.button("Se connecter à OneDrive", type="primary"):
        with st.spinner("Initialisation..."):
            try:
                st.session_state.device_flow = start_device_flow()
            except Exception as e:
                st.error(f"Erreur : {e}")
    if st.session_state.device_flow:
        flow = st.session_state.device_flow
        m    = re.search(r'enter the code ([A-Z0-9]+)', flow.get("message", ""))
        code = m.group(1) if m else ""
        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown("**1.** Ouvrez : [https://microsoft.com/devicelogin](https://microsoft.com/devicelogin)")
            st.markdown("**2.** Entrez le code ci-contre")
            st.markdown("**3.** Connectez-vous avec votre compte Microsoft")
        with col2:
            st.info(f"Code :\n# {code}")
        if st.button("J'ai validé le code ✓", type="primary"):
            with st.spinner("Vérification..."):
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

# Chargement unique de l'index au démarrage
if "cache_index" not in st.session_state:
    load_index(token)


# ──────────────────────────────────────────
# SIDEBAR — stats mises en cache
# ──────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_sidebar_stats(n):
    idx = st.session_state.get("cache_index", [])
    types = {}
    for f in idx:
        t = f.get("type", "Autre")
        types[t] = types.get(t, 0) + 1
    return types

with st.sidebar:
    st.title("🔍 OneDrive Search Pro")
    n = len(st.session_state.get("cache_index", []))
    st.metric("Fichiers indexés", n)
    if n > 0:
        for t, c in sorted(get_sidebar_stats(n).items(), key=lambda x: -x[1])[:6]:
            st.caption(f"{c} {t}")
    st.divider()
    if st.button("🔄 Recharger l'index", use_container_width=True):
        st.session_state.pop("cache_index", None)
        get_sidebar_stats.clear()
        try:
            os.remove(LOCAL_CACHE)
        except Exception:
            pass
        st.rerun()
    if st.button("🚪 Déconnecter", use_container_width=True):
        for k in ["token", "device_flow", "authenticated", "cache_index"]:
            st.session_state.pop(k, None)
        get_sidebar_stats.clear()
        st.rerun()


# ──────────────────────────────────────────
# ONGLETS avec @st.fragment
# Chaque fragment est isolé : cliquer dans un onglet
# ne rerun PAS les autres onglets ni le sidebar.
# ──────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["🔍 Recherche", "📂 Parcourir", "🗂️ Indexation"])


# ════════════════════════════════════════
# ONGLET 1 — RECHERCHE
# ════════════════════════════════════════
@st.fragment
def render_search():
    st.subheader("Recherche dans vos fichiers OneDrive")

    col_q, col_mode = st.columns([3, 1])
    with col_q:
        query = st.text_input("", placeholder="Rechercher un mot-clé...",
                              label_visibility="collapsed")
    with col_mode:
        search_mode = st.selectbox("Mode", ["Index local", "Graph Search", "Les deux"],
                                   label_visibility="collapsed")

    if "keywords" not in st.session_state:
        st.session_state.keywords = [""]

    st.markdown("**Mots-clés** *(tous doivent correspondre — logique ET)*")
    to_remove = None
    for i, kw in enumerate(st.session_state.keywords):
        col_kw, col_del = st.columns([8, 1])
        with col_kw:
            st.session_state.keywords[i] = st.text_input(
                f"Mot-clé {i+1}", value=kw,
                placeholder="Entrez un mot-clé...",
                key=f"kw_{i}", label_visibility="collapsed")
        with col_del:
            if len(st.session_state.keywords) > 1:
                if st.button("✕", key=f"del_{i}"):
                    to_remove = i
    if to_remove is not None:
        st.session_state.keywords.pop(to_remove)
        st.rerun()

    col_add, col_search = st.columns([2, 3])
    with col_add:
        if st.button("➕ Ajouter un mot-clé", use_container_width=True):
            st.session_state.keywords.append("")
            st.rerun()
    with col_search:
        do_search = st.button("🔍 Rechercher", type="primary", use_container_width=True)

    active_keywords = [k.strip() for k in st.session_state.keywords if k.strip()]

    with st.expander("🎛️ Filtres"):
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            filter_types = st.multiselect("Types de fichiers",
                options=list(set(FILE_TYPES.keys())), default=[],
                placeholder="Tous les types")
        with col_f2:
            filter_date = st.selectbox("Modifié depuis",
                ["Toujours", "Aujourd'hui", "Cette semaine", "Ce mois", "Cette année"])
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

    if do_search and active_keywords:
        results = []
        with st.spinner("Recherche en cours..."):
            if search_mode in ("Index local", "Les deux"):
                results.extend(search_local_index(
                    token, active_keywords,
                    file_types=filter_types if filter_types else None,
                    search_in_name=show_name_match,
                    search_in_content=show_content_match,
                    search_in_path=show_path_match,
                ))
            if search_mode in ("Graph Search", "Les deux"):
                graph_query = " AND ".join(f'"{k}"' for k in active_keywords)
                graph = search_graph(token, graph_query,
                                     file_types=filter_types if filter_types else None)
                local_ids = {r["id"] for r in results}
                for g in graph:
                    if g["id"] not in local_ids:
                        results.append(g)

        if filter_date != "Toujours":
            now = datetime.datetime.now()
            cutoffs = {
                "Aujourd'hui":  now - datetime.timedelta(days=1),
                "Cette semaine": now - datetime.timedelta(days=7),
                "Ce mois":      now - datetime.timedelta(days=30),
                "Cette année":  now - datetime.timedelta(days=365),
            }
            cutoff = cutoffs.get(filter_date)
            if cutoff:
                results = [r for r in results
                           if r.get("modified", "") >= cutoff.strftime("%Y-%m-%d")]
        if filter_path:
            results = [r for r in results
                       if filter_path.lower() in r.get("path", "").lower()]

        kw_label = " + ".join(f"«\u202f{k}\u202f»" for k in active_keywords)
        if not results:
            st.info(f"Aucun résultat pour {kw_label}")
        else:
            st.success(f"**{len(results)} fichier(s) trouvé(s)** pour {kw_label}")
            for r in results:
                icon = r.get("icon", "📁")
                name = r.get("name", "")
                mod  = r.get("modified", "")
                size = format_size(r.get("size", 0))
                typ  = r.get("type", "Fichier")
                path = r.get("path", "")
                url  = r.get("url", "")
                badges = []
                if r.get("match_name"):    badges.append("🏷️ Nom")
                if r.get("match_content"): badges.append("📄 Contenu")
                if r.get("match_path"):    badges.append("📂 Chemin")
                if r.get("source") == "graph_search": badges.append("🔎 Graph")
                with st.expander(f"{icon} **{name}** — {size} — {mod}  {'  '.join(badges)}"):
                    col_i, col_a = st.columns([3, 1])
                    with col_i:
                        st.markdown(f"**Type :** {typ}")
                        st.markdown(f"**Chemin :** `{path}/{name}`")
                        st.markdown(f"**Modifié le :** {mod} | **Taille :** {size}")
                        if r.get("excerpt"):
                            highlighted = r["excerpt"]
                            for kw in (r.get("keywords") or active_keywords):
                                highlighted = re.sub(f"({re.escape(kw)})", r"**\1**",
                                                     highlighted, flags=re.IGNORECASE)
                            st.markdown(f"**Extrait :**\n> {highlighted}")
                        elif r.get("summary"):
                            st.markdown(f"**Extrait :**\n> {r['summary']}")
                    with col_a:
                        if url:
                            st.link_button("📂 Ouvrir", url, use_container_width=True)
    elif do_search:
        st.warning("Saisissez au moins un mot-clé.")


# ════════════════════════════════════════
# ONGLET 2 — PARCOURIR
# ════════════════════════════════════════
@st.fragment
def render_browse():
    st.subheader("Parcourir les fichiers indexés")
    idx = st.session_state.get("cache_index", [])
    if not idx:
        st.info("Aucun fichier indexé. Lancez l'indexation depuis l'onglet Indexation.")
        return

    col_b1, col_b2, col_b3 = st.columns([2, 1, 1])
    with col_b1:
        browse_search = st.text_input("Filtrer", placeholder="nom, dossier...")
    with col_b2:
        browse_type = st.selectbox("Type", ["Tous"] + sorted(set(
            f.get("type", "Autre") for f in idx)))
    with col_b3:
        browse_sort = st.selectbox("Trier par", ["Nom", "Date", "Taille", "Type"])

    filtered = idx
    if browse_search:
        bl = browse_search.lower()
        filtered = [f for f in filtered if
                    bl in str(f.get("name", "")).lower() or
                    bl in str(f.get("path", "")).lower()]
    if browse_type != "Tous":
        filtered = [f for f in filtered if f.get("type") == browse_type]

    sort_keys = {
        "Nom":    lambda x: str(x.get("name", "")).lower(),
        "Date":   lambda x: str(x.get("modified", "")),
        "Taille": lambda x: x.get("size", 0),
        "Type":   lambda x: str(x.get("type", ""))
    }
    filtered.sort(key=sort_keys[browse_sort], reverse=(browse_sort in ["Date", "Taille"]))
    st.write(f"**{len(filtered)} fichier(s)**")

    folders = {}
    for f in filtered:
        p = f.get("path", "/") or "/"
        folders.setdefault(p, []).append(f)

    for folder_path in sorted(folders.keys()):
        files = folders[folder_path]
        with st.expander(f"📁 `{folder_path or '/'}` — {len(files)} fichier(s)"):
            for f in files:
                col_n, col_s, col_d, col_a = st.columns([4, 1, 1, 1])
                with col_n:
                    st.markdown(f"{f.get('icon','📁')} **{f.get('name','')}**")
                with col_s:
                    st.caption(format_size(f.get("size", 0)))
                with col_d:
                    st.caption(f.get("modified", ""))
                with col_a:
                    if f.get("url"):
                        st.link_button("Ouvrir", f["url"], use_container_width=True)


# ════════════════════════════════════════
# ONGLET 3 — INDEXATION
# ════════════════════════════════════════
@st.fragment
def render_index():
    st.subheader("Indexation de votre OneDrive")
    idx = st.session_state.get("cache_index", [])

    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        st.metric("Fichiers indexés", len(idx))
    with col_m2:
        st.metric("Avec contenu extrait", sum(1 for f in idx if f.get("content")))
    with col_m3:
        last = max((f.get("indexed", "") for f in idx), default="") if idx else ""
        st.metric("Dernière indexation", last[:10] if last else "Jamais")

    st.divider()
    st.info("Parcourt tout votre OneDrive et extrait le texte (Word, Excel, PowerPoint, TXT). "
            "Les PDFs sont référencés sans extraction — utilisez Graph Search pour chercher dans leur contenu. "
            "Fichiers > 5 Mo référencés sans extraction.")

    col_o1, col_o2 = st.columns(2)
    with col_o1:
        extract_content = st.checkbox("Extraire le contenu des fichiers", value=True)
    with col_o2:
        incremental = st.checkbox("Indexation incrémentale (ignorer les fichiers inchangés)", value=True)

    autosave_every = st.slider("Sauvegarde automatique toutes les N fichiers",
                               min_value=10, max_value=200, value=50, step=10)
    st.caption("💡 L'index est sauvegardé automatiquement. "
               "Vous pouvez fermer le navigateur et reprendre avec l'incrémentale.")

    if st.button("🚀 Lancer l'indexation", type="primary"):
        bar         = st.progress(0, text="Démarrage...")
        info        = st.empty()
        save_status = st.empty()
        t_start     = time.time()

        existing_map  = {f["id"]: f.get("modified", "") for f in idx} if incremental else {}
        running_index = list(idx) if incremental else []
        new_index     = []
        skipped_count = [0]
        indexable     = {"docx", "doc", "xlsx", "xls", "pptx", "ppt", "txt", "csv"}

        def scan(folder_id, current_path):
            try:
                endpoint  = ("/me/drive/root/children" if folder_id == "root"
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
                    for item in data.get("value", []):
                        name = item.get("name", "")
                        if item.get("folder"):
                            scan(item["id"], f"{current_path}/{name}")
                        elif item.get("file"):
                            item_id  = item["id"]
                            modified = item.get("lastModifiedDateTime", "")[:10]
                            if incremental and item_id in existing_map:
                                if existing_map[item_id] == modified:
                                    skipped_count[0] += 1
                                    continue
                                running_index[:] = [f for f in running_index
                                                    if f["id"] != item_id]
                            ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                            size = item.get("size", 0)
                            content = ""
                            if extract_content and ext in indexable and size < 5*1024*1024:
                                content = extract_text_from_file(token, item_id, ext)
                            entry = {
                                "id": item_id, "name": name, "ext": ext,
                                "icon":     FILE_TYPES.get(ext, {}).get("icon", "📁"),
                                "type":     FILE_TYPES.get(ext, {}).get("label", "Fichier"),
                                "path":     current_path,
                                "url":      item.get("webUrl", ""),
                                "size":     size, "modified": modified,
                                "content":  content,
                                "indexed":  datetime.datetime.now().isoformat()
                            }
                            new_index.append(entry)
                            running_index.append(entry)
                            n = len(new_index)
                            info.markdown(f"**{n} nouveaux** | ⏭️ {skipped_count[0]} ignorés | "
                                          f"`{current_path}/{name}`")
                            bar.progress(min(n / 500, 1.0))
                            if n % autosave_every == 0:
                                try:
                                    save_index(token, running_index)
                                    save_status.success(f"💾 {n} fichiers sauvegardés ✓")
                                except Exception as e_save:
                                    save_status.warning(f"⚠️ Sauvegarde échouée : {e_save}")
                    next_link = data.get("@odata.nextLink")
            except Exception as e:
                info.warning(f"Erreur dossier {current_path} : {e}")

        scan_error = None
        try:
            scan("root", "")
        except Exception as e:
            scan_error = e

        duration = int(time.time() - t_start)
        saved_ok = False
        save_error = None
        if running_index:
            try:
                save_index(token, running_index)
                get_sidebar_stats.clear()
                saved_ok = True
            except Exception as e:
                save_error = e

        bar.progress(1.0, text="Terminé !")
        save_status.empty()

        if saved_ok:
            st.success(f"Terminé en {duration}s — **{len(new_index)} nouveaux fichiers** "
                       f"(total : {len(running_index)}, ignorés : {skipped_count[0]})")
            if scan_error:
                st.warning(f"Scan interrompu : {scan_error}")
            st.balloons()
        else:
            st.error(f"Sauvegarde échouée : {save_error}")
            if running_index:
                st.download_button("⬇️ Télécharger l'index (JSON)",
                    data=json.dumps(running_index, ensure_ascii=False, indent=2),
                    file_name="file_index.json", mime="application/json")

    st.divider()
    if idx and st.button("🗑️ Supprimer l'index", type="secondary"):
        if st.session_state.get("confirm_delete"):
            save_index(token, [])
            get_sidebar_stats.clear()
            st.session_state.pop("confirm_delete", None)
            st.success("Index supprimé.")
            st.rerun()
        else:
            st.session_state["confirm_delete"] = True
            st.warning("Cliquez à nouveau pour confirmer.")


# ── Rendu dans les onglets ──
with tab1:
    render_search()
with tab2:
    render_browse()
with tab3:
    render_index()
