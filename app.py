"""
OneDrive Search Pro
Recherche intelligente dans vos fichiers OneDrive (PDF, Word, Excel, PowerPoint, etc.)

Secrets Streamlit requis :
  AZURE_CLIENT_ID    = "..."
  AZURE_TENANT_ID    = "common"
  APP_PASSWORD       = "..."
"""

import os
import re
import io
import json
import time
import zipfile
import datetime
import requests
import streamlit as st

# ─────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────
GRAPH           = "https://graph.microsoft.com/v1.0"
OD_FOLDER       = "OneDriveSearch"
INDEX_FILE      = "file_index.json"
LOCAL_CACHE     = "/tmp/od_index.json"
MAX_CHARS       = 3_000   # texte max stocké par fichier
MAX_FILE_BYTES  = 5_000_000  # fichiers > 5 Mo : pas d'extraction

FILE_META = {
    "pdf":  ("📄", "PDF"),
    "docx": ("📝", "Word"),   "doc":  ("📝", "Word"),
    "xlsx": ("📊", "Excel"),  "xls":  ("📊", "Excel"),
    "pptx": ("📑", "PPT"),    "ppt":  ("📑", "PPT"),
    "txt":  ("📃", "Texte"),  "csv":  ("📊", "CSV"),
    "msg":  ("✉️",  "Email"),  "eml":  ("✉️",  "Email"),
    "png":  ("🖼️",  "Image"),  "jpg":  ("🖼️",  "Image"),
    "jpeg": ("🖼️",  "Image"),  "zip":  ("🗜️",  "Archive"),
    "mp4":  ("🎬",  "Vidéo"),
}
EXTRACTABLE = {"docx", "doc", "xlsx", "xls", "pptx", "ppt", "txt", "csv"}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def file_icon(ext):  return FILE_META.get(ext, ("📁", "Fichier"))[0]
def file_type(ext):  return FILE_META.get(ext, ("📁", "Fichier"))[1]
def file_ext(name):  return name.rsplit(".", 1)[-1].lower() if "." in name else ""

def fmt_size(b):
    if b < 1024:         return f"{b} o"
    if b < 1_048_576:    return f"{b/1024:.1f} Ko"
    if b < 1_073_741_824: return f"{b/1_048_576:.1f} Mo"
    return f"{b/1_073_741_824:.1f} Go"

def clean(text):
    text = re.sub(r"[^\x09\x0a\x0d\x20-\x7e\x80-\xff]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def readable(text, ratio=0.80):
    if not text: return False
    ok = sum(1 for c in text if c.isprintable() or c in "\n\r\t")
    return ok / len(text) >= ratio


# ─────────────────────────────────────────────────────────────
# EXTRACTION DE TEXTE
# ─────────────────────────────────────────────────────────────
def extract_text(token, item_id, ext, size):
    """Télécharge et extrait le texte. PDFs exclus (Graph Search les couvre)."""
    if ext not in EXTRACTABLE or size > MAX_FILE_BYTES:
        return ""
    try:
        r = requests.get(
            f"{GRAPH}/me/drive/items/{item_id}/content",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20, stream=True, allow_redirects=True)
        if r.status_code != 200:
            return ""
        chunks, total = [], 0
        for chunk in r.iter_content(32_768):
            chunks.append(chunk); total += len(chunk)
            if total >= MAX_FILE_BYTES: break
        raw = b"".join(chunks)

        if ext in ("txt", "csv"):
            t = raw.decode("utf-8", errors="ignore")[:MAX_CHARS]
            return t if readable(t) else ""

        z = zipfile.ZipFile(io.BytesIO(raw))

        if ext in ("docx", "doc"):
            xml = z.read("word/document.xml").decode("utf-8", errors="ignore")
            t = clean(re.sub(r"<[^>]+>", " ", xml))[:MAX_CHARS]
            return t if readable(t) else ""

        if ext in ("xlsx", "xls"):
            parts = []
            for n in z.namelist():
                if n.startswith("xl/worksheets/") and n.endswith(".xml"):
                    xml = z.read(n).decode("utf-8", errors="ignore")
                    parts.append(clean(re.sub(r"<[^>]+>", " ", xml)))
                    if sum(len(p) for p in parts) > MAX_CHARS: break
            t = " ".join(parts)[:MAX_CHARS]
            return t if readable(t) else ""

        if ext in ("pptx", "ppt"):
            parts = []
            for n in sorted(z.namelist()):
                if n.startswith("ppt/slides/slide") and n.endswith(".xml"):
                    xml = z.read(n).decode("utf-8", errors="ignore")
                    parts.append(clean(re.sub(r"<[^>]+>", " ", xml)))
                    if sum(len(p) for p in parts) > MAX_CHARS: break
            t = " ".join(parts)[:MAX_CHARS]
            return t if readable(t) else ""

    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────────
# INDEX — STOCKAGE (mémoire > /tmp > OneDrive)
# ─────────────────────────────────────────────────────────────
def _od_get(token, path):
    r = requests.get(
        f"{GRAPH}/me/drive/root:/{OD_FOLDER}/{path}:/content",
        headers={"Authorization": f"Bearer {token}"}, timeout=30)
    if r.status_code == 404: return None
    r.raise_for_status()
    return r.json()

def _od_put(token, path, data):
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
    r = requests.put(
        f"{GRAPH}/me/drive/root:/{OD_FOLDER}/{path}:/content",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=body, timeout=max(30, len(body) // 50_000 + 20))
    if r.status_code not in (200, 201):
        raise RuntimeError(f"OneDrive {r.status_code}: {r.text[:200]}")

def _read_tmp():
    try:
        if os.path.exists(LOCAL_CACHE):
            with open(LOCAL_CACHE, encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, list) else None
    except Exception:
        return None

def _write_tmp(data):
    try:
        with open(LOCAL_CACHE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        pass

def load_index(token):
    """Charge l'index : session_state → /tmp → OneDrive."""
    if "idx" in st.session_state:
        return st.session_state.idx
    tmp = _read_tmp()
    if tmp is not None:
        st.session_state.idx = tmp
        return tmp
    with st.spinner("Chargement de l'index depuis OneDrive…"):
        data = _od_get(token, INDEX_FILE)
    result = data if isinstance(data, list) else []
    st.session_state.idx = result
    _write_tmp(result)
    return result

def save_index(token, data):
    """Sauvegarde : mémoire + /tmp immédiatement, OneDrive en arrière-plan."""
    st.session_state.idx = data
    _write_tmp(data)
    for attempt in range(3):
        try:
            _od_put(token, INDEX_FILE, data)
            return
        except Exception as e:
            if attempt == 2:
                raise RuntimeError(f"Sauvegarde OneDrive échouée : {e}")
            time.sleep(2)


# ─────────────────────────────────────────────────────────────
# RECHERCHE LOCALE (multi-mots-clés, logique ET)
# ─────────────────────────────────────────────────────────────
def search_local(token, keywords, exts=None, in_name=True, in_content=True, in_path=False):
    kws = [k.lower().strip() for k in keywords if k.strip()]
    if not kws or not (in_name or in_content or in_path):
        return []

    idx = load_index(token)
    results = []

    for item in idx:
        if exts and item.get("ext", "") not in exts:
            continue

        name_s    = item.get("name", "").lower()
        path_s    = item.get("path", "").lower()
        content_s = (item.get("content") or "").lower()

        # Tous les mots-clés doivent matcher dans au moins un champ ACTIF
        def found(kw):
            if in_name    and kw in name_s:    return True
            if in_path    and kw in path_s:    return True
            if in_content and kw in content_s: return True
            return False

        if not all(found(kw) for kw in kws):
            continue

        hit_name    = in_name    and any(kw in name_s    for kw in kws)
        hit_path    = in_path    and any(kw in path_s    for kw in kws)
        hit_content = in_content and any(kw in content_s for kw in kws)

        entry = dict(item)
        entry.update(hit_name=hit_name, hit_path=hit_path,
                     hit_content=hit_content, keywords=kws, source="local")

        # Extrait contextuel
        if hit_content:
            full = item.get("content") or ""
            kw   = next((k for k in kws if k in full.lower()), kws[0])
            i    = full.lower().find(kw)
            entry["excerpt"] = "…" + full[max(0,i-80):min(len(full),i+120)] + "…"
        elif hit_path:
            kw = next((k for k in kws if k in path_s), kws[0])
            i  = path_s.find(kw)
            entry["excerpt"] = "…" + path_s[max(0,i-40):min(len(path_s),i+80)] + "…"

        results.append(entry)

    results.sort(key=lambda x: (not x["hit_name"], not x["hit_content"], not x["hit_path"]))
    return results


# ─────────────────────────────────────────────────────────────
# RECHERCHE GRAPH API (Microsoft, contenu PDF inclus)
# ─────────────────────────────────────────────────────────────
def search_graph(token, keywords, exts=None):
    kql = " AND ".join(f'"{k}"' for k in keywords)
    if exts:
        kql = f"({kql}) AND ({' OR '.join(f'filetype:{e}' for e in exts)})"
    try:
        resp = requests.post(f"{GRAPH}/search/query",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"requests": [{"entityTypes": ["driveItem"], "query": {"queryString": kql},
                                "from": 0, "size": 50,
                                "fields": ["id","name","webUrl","lastModifiedDateTime",
                                           "size","parentReference","file"]}]},
            timeout=15)
        if resp.status_code != 200:
            return []
        hits = (resp.json().get("value",[{}])[0]
                           .get("hitsContainers",[{}])[0]
                           .get("hits",[]))
        results = []
        for h in hits:
            res  = h.get("resource", {})
            name = res.get("name", "")
            ext  = file_ext(name)
            results.append({
                "id":       res.get("id",""),
                "name":     name,
                "ext":      ext,
                "path":     res.get("parentReference",{}).get("path","").replace("/drive/root:",""),
                "url":      res.get("webUrl",""),
                "modified": res.get("lastModifiedDateTime","")[:10],
                "size":     res.get("size", 0),
                "summary":  h.get("summary",""),
                "source":   "graph",
            })
        return results
    except Exception as e:
        st.warning(f"Graph Search : {e}")
        return []


# ─────────────────────────────────────────────────────────────
# CONNEXION MICROSOFT (Device Flow)
# ─────────────────────────────────────────────────────────────
def start_flow():
    r = requests.post(
        f"https://login.microsoftonline.com/{st.secrets['AZURE_TENANT_ID']}"
        f"/oauth2/v2.0/devicecode",
        data={"client_id": st.secrets["AZURE_CLIENT_ID"],
              "scope": "Files.Read.All Files.ReadWrite offline_access"},
        timeout=15)
    r.raise_for_status()
    return r.json()

def poll_flow(device_code):
    r = requests.post(
        f"https://login.microsoftonline.com/{st.secrets['AZURE_TENANT_ID']}"
        f"/oauth2/v2.0/token",
        data={"client_id": st.secrets["AZURE_CLIENT_ID"],
              "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
              "device_code": device_code},
        timeout=15)
    d = r.json()
    return d.get("access_token")


# ═════════════════════════════════════════════════════════════
# INTERFACE
# ═════════════════════════════════════════════════════════════
st.set_page_config(page_title="OneDrive Search Pro", page_icon="🔍", layout="wide")

# ── Mot de passe ──────────────────────────────────────────────
if not st.session_state.get("auth"):
    st.title("🔍 OneDrive Search Pro")
    _, col, _ = st.columns([1, 2, 1])
    with col:
        pwd = st.text_input("Mot de passe", type="password")
        if st.button("Entrer", type="primary", use_container_width=True):
            if pwd == st.secrets.get("APP_PASSWORD", ""):
                st.session_state.auth = True
                st.rerun()
            else:
                st.error("Mot de passe incorrect.")
    st.stop()

# ── Connexion OneDrive ────────────────────────────────────────
if not st.session_state.get("token"):
    st.title("🔍 OneDrive Search Pro")

    if st.button("🔗 Se connecter à OneDrive", type="primary"):
        try:
            st.session_state.flow = start_flow()
        except Exception as e:
            st.error(f"Erreur : {e}")

    if st.session_state.get("flow"):
        flow = st.session_state.flow
        m    = re.search(r"enter the code ([A-Z0-9]+)", flow.get("message",""), re.I)
        code = m.group(1) if m else "—"
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown("**1.** Ouvrez [microsoft.com/devicelogin](https://microsoft.com/devicelogin)")
            st.markdown("**2.** Entrez ce code, connectez-vous avec votre compte Microsoft")
        with c2:
            st.info(f"## {code}")

        if st.button("✅ J'ai validé", type="primary"):
            with st.spinner("Vérification…"):
                for _ in range(30):
                    tok = poll_flow(flow["device_code"])
                    if tok:
                        st.session_state.token = tok
                        st.rerun()
                    time.sleep(3)
                st.error("Délai dépassé. Rechargez la page.")
    st.stop()

token = st.session_state.token

# Chargement unique de l'index (ne se retélécharge pas à chaque rerun)
if "idx" not in st.session_state:
    load_index(token)


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def sidebar_stats(n_files):
    idx = st.session_state.get("idx", [])
    types = {}
    for f in idx:
        t = file_type(f.get("ext",""))
        types[t] = types.get(t, 0) + 1
    return sorted(types.items(), key=lambda x: -x[1])[:8]

with st.sidebar:
    st.markdown("## 🔍 OneDrive Search")
    n = len(st.session_state.get("idx", []))
    st.metric("Fichiers indexés", n)
    if n:
        for t, c in sidebar_stats(n):
            st.caption(f"{c}  ·  {t}")
    st.divider()
    if st.button("🔄 Recharger l'index", use_container_width=True):
        st.session_state.pop("idx", None)
        sidebar_stats.clear()
        try: os.remove(LOCAL_CACHE)
        except Exception: pass
        st.rerun()
    if st.button("🚪 Déconnecter", use_container_width=True):
        for k in ["token", "flow", "auth", "idx"]:
            st.session_state.pop(k, None)
        sidebar_stats.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────
# ONGLETS  (chaque @st.fragment est isolé : zéro rerun des autres)
# ─────────────────────────────────────────────────────────────
tab_search, tab_browse, tab_index = st.tabs(["🔍 Recherche", "📂 Parcourir", "🗂️ Indexation"])


# ════════════════════════════════════════
# ONGLET RECHERCHE
# ════════════════════════════════════════
@st.fragment
def tab_recherche():
    st.subheader("Recherche dans vos fichiers OneDrive")

    # ── Mode ──
    search_mode = st.selectbox("Mode de recherche",
        ["Index local", "Graph Search (contenu PDF inclus)", "Les deux"],
        label_visibility="visible")

    # ── Mots-clés ──
    st.markdown("**Mots-clés** — tous doivent correspondre *(logique ET)*")
    if "kws" not in st.session_state:
        st.session_state.kws = [""]

    to_del = None
    for i, kw in enumerate(st.session_state.kws):
        ca, cb = st.columns([9, 1])
        with ca:
            st.session_state.kws[i] = st.text_input(
                f"kw{i}", value=kw, placeholder=f"Mot-clé {i+1}…",
                key=f"kw_input_{i}", label_visibility="collapsed")
        with cb:
            if len(st.session_state.kws) > 1 and st.button("✕", key=f"kw_del_{i}"):
                to_del = i

    if to_del is not None:
        st.session_state.kws.pop(to_del)
        st.rerun()

    ca, cb = st.columns([1, 2])
    with ca:
        if st.button("➕ Ajouter un mot-clé", use_container_width=True):
            st.session_state.kws.append("")
            st.rerun()
    with cb:
        go = st.button("🔍 Rechercher", type="primary", use_container_width=True)

    active = [k.strip() for k in st.session_state.kws if k.strip()]

    # ── Filtres ──
    with st.expander("🎛️ Filtres"):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            filt_exts = st.multiselect("Types de fichiers",
                sorted(FILE_META.keys()), placeholder="Tous")
        with fc2:
            filt_date = st.selectbox("Modifié depuis",
                ["Toujours","Aujourd'hui","Cette semaine","Ce mois","Cette année"])
        with fc3:
            filt_folder = st.text_input("Dans le dossier", placeholder="ex: Documents")

        st.markdown("**Chercher dans :**")
        fc4, fc5, fc6 = st.columns(3)
        with fc4: in_name    = st.checkbox("📛 Nom du fichier",  value=True)
        with fc5: in_content = st.checkbox("📄 Contenu",         value=True)
        with fc6: in_path    = st.checkbox("📂 Chemin",          value=False)

    # ── Lancement ──
    if not go:
        return
    if not active:
        st.warning("Saisissez au moins un mot-clé.")
        return

    results = []
    with st.spinner("Recherche…"):
        use_local = "Index local" in search_mode or "Les deux" in search_mode
        use_graph = "Graph" in search_mode or "Les deux" in search_mode

        if use_local:
            results = search_local(token, active,
                exts=filt_exts or None,
                in_name=in_name, in_content=in_content, in_path=in_path)

        if use_graph:
            seen = {r["id"] for r in results}
            for g in search_graph(token, active, exts=filt_exts or None):
                if g["id"] not in seen:
                    results.append(g)

    # Filtre date
    if filt_date != "Toujours":
        now = datetime.datetime.now()
        deltas = {"Aujourd'hui":1,"Cette semaine":7,"Ce mois":30,"Cette année":365}
        cutoff = (now - datetime.timedelta(days=deltas[filt_date])).strftime("%Y-%m-%d")
        results = [r for r in results if r.get("modified","") >= cutoff]

    # Filtre dossier
    if filt_folder:
        fl = filt_folder.lower()
        results = [r for r in results if fl in r.get("path","").lower()]

    # ── Affichage ──
    label = " + ".join(f"«{k}»" for k in active)
    if not results:
        st.info(f"Aucun résultat pour {label}")
        return

    st.success(f"**{len(results)} résultat(s)** pour {label}")

    for r in results:
        ext  = r.get("ext","")
        name = r.get("name","")
        mod  = r.get("modified","")
        size = fmt_size(r.get("size",0))
        path = r.get("path","")
        url  = r.get("url","")

        tags = []
        if r.get("hit_name"):    tags.append("🏷️ Nom")
        if r.get("hit_content"): tags.append("📄 Contenu")
        if r.get("hit_path"):    tags.append("📂 Chemin")
        if r.get("source") == "graph": tags.append("🔎 Graph")

        header = f"{file_icon(ext)} **{name}** — {size} — {mod}  {'  '.join(tags)}"
        with st.expander(header):
            ci, ca = st.columns([3, 1])
            with ci:
                st.markdown(f"**Type :** {file_type(ext)}")
                st.markdown(f"**Chemin :** `{path}/{name}`")
                st.markdown(f"**Modifié le :** {mod} · **Taille :** {size}")
                excerpt = r.get("excerpt") or r.get("summary","")
                if excerpt:
                    hl = excerpt
                    for kw in (r.get("keywords") or active):
                        hl = re.sub(f"({re.escape(kw)})", r"**\1**", hl, flags=re.I)
                    st.markdown(f"**Extrait :**\n> {hl}")
            with ca:
                if url:
                    st.link_button("📂 Ouvrir", url, use_container_width=True)


# ════════════════════════════════════════
# ONGLET PARCOURIR
# ════════════════════════════════════════
@st.fragment
def tab_parcourir():
    st.subheader("Parcourir les fichiers indexés")
    idx = st.session_state.get("idx", [])
    if not idx:
        st.info("Aucun fichier indexé — lancez l'indexation.")
        return

    ca, cb, cc = st.columns([3, 1, 1])
    with ca: search = st.text_input("🔎 Filtrer", placeholder="nom ou dossier…")
    with cb:
        ftype = st.selectbox("Type", ["Tous"] + sorted({file_type(f.get("ext","")) for f in idx}))
    with cc:
        sort  = st.selectbox("Trier par", ["Nom","Date","Taille","Type"])

    filtered = [f for f in idx
                if (not search or
                    search.lower() in f.get("name","").lower() or
                    search.lower() in f.get("path","").lower())
                and (ftype == "Tous" or file_type(f.get("ext","")) == ftype)]

    keys = {"Nom": lambda x: x.get("name","").lower(),
            "Date": lambda x: x.get("modified",""),
            "Taille": lambda x: x.get("size",0),
            "Type": lambda x: x.get("ext","")}
    filtered.sort(key=keys[sort], reverse=sort in ("Date","Taille"))

    st.caption(f"{len(filtered)} fichier(s)")

    folders: dict = {}
    for f in filtered:
        folders.setdefault(f.get("path","") or "/", []).append(f)

    for fpath in sorted(folders):
        files = folders[fpath]
        with st.expander(f"📁 `{fpath}` — {len(files)} fichier(s)"):
            for f in files:
                c1, c2, c3, c4 = st.columns([4, 1, 1, 1])
                c1.markdown(f"{file_icon(f.get('ext',''))} **{f.get('name','')}**")
                c2.caption(fmt_size(f.get("size",0)))
                c3.caption(f.get("modified",""))
                if f.get("url"):
                    c4.link_button("Ouvrir", f["url"], use_container_width=True)


# ════════════════════════════════════════
# ONGLET INDEXATION
# ════════════════════════════════════════
@st.fragment
def tab_indexation():
    st.subheader("Indexation de votre OneDrive")
    idx = st.session_state.get("idx", [])

    c1, c2, c3 = st.columns(3)
    c1.metric("Fichiers indexés", len(idx))
    c2.metric("Avec contenu", sum(1 for f in idx if f.get("content")))
    last = max((f.get("indexed","") for f in idx), default="") if idx else ""
    c3.metric("Dernière indexation", last[:10] if last else "—")

    st.divider()
    st.info(
        "📄 **PDFs** : référencés (nom + chemin). Utilisez **Graph Search** pour chercher "
        "dans leur contenu — Microsoft l'indexe nativement. \n\n"
        "📝 **Word, Excel, PPT, TXT** : contenu extrait et indexé localement."
    )

    ca, cb = st.columns(2)
    with ca: extract  = st.checkbox("Extraire le contenu (Word, Excel, PPT, TXT)", value=True)
    with cb: incremental = st.checkbox("Mode incrémental (ignorer les fichiers inchangés)", value=True)

    autosave = st.slider("Sauvegarder automatiquement tous les N fichiers",
                         min_value=25, max_value=200, value=50, step=25)

    st.caption("💡 La progression est sauvegardée régulièrement. "
               "Vous pouvez fermer le navigateur et reprendre en mode incrémental.")

    if not st.button("🚀 Lancer l'indexation", type="primary"):
        if idx:
            st.divider()
            if st.button("🗑️ Supprimer l'index", type="secondary"):
                if st.session_state.get("confirm_del"):
                    save_index(token, [])
                    sidebar_stats.clear()
                    st.session_state.pop("confirm_del", None)
                    st.success("Index supprimé.")
                    st.rerun()
                else:
                    st.session_state.confirm_del = True
                    st.warning("Cliquez à nouveau pour confirmer.")
        return

    # ── Scan ──
    bar    = st.progress(0, text="Démarrage…")
    status = st.empty()
    saved  = st.empty()

    known    = {f["id"]: f.get("modified","") for f in idx} if incremental else {}
    running  = list(idx) if incremental else []
    new_idx  = []
    skipped  = [0]
    t0       = time.time()

    def scan(folder_id, path):
        ep   = ("/me/drive/root/children" if folder_id == "root"
                else f"/me/drive/items/{folder_id}/children")
        link = (f"{GRAPH}{ep}?$top=200"
                "&$select=id,name,size,file,folder,lastModifiedDateTime,webUrl,parentReference")
        while link:
            try:
                r = requests.get(link, headers={"Authorization": f"Bearer {token}"}, timeout=20)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                status.warning(f"Erreur {path} : {e}")
                return

            for item in data.get("value", []):
                name = item.get("name","")
                if item.get("folder"):
                    scan(item["id"], f"{path}/{name}")
                    continue
                if not item.get("file"):
                    continue

                iid = item["id"]
                mod = item.get("lastModifiedDateTime","")[:10]

                if incremental and iid in known:
                    if known[iid] == mod:
                        skipped[0] += 1
                        continue
                    # Fichier modifié : on remplace l'entrée
                    running[:] = [f for f in running if f["id"] != iid]

                ext  = file_ext(name)
                size = item.get("size", 0)
                content = extract_text(token, iid, ext, size) if extract else ""

                entry = {
                    "id": iid, "name": name, "ext": ext,
                    "path": path, "url": item.get("webUrl",""),
                    "size": size, "modified": mod, "content": content,
                    "indexed": datetime.datetime.now().isoformat(),
                }
                new_idx.append(entry)
                running.append(entry)

                n = len(new_idx)
                bar.progress(min(n / 500, 1.0))
                status.markdown(f"**{n} nouveaux** · ⏭️ {skipped[0]} ignorés · `{path}/{name}`")

                if n % autosave == 0:
                    try:
                        save_index(token, running)
                        saved.success(f"💾 Sauvegarde automatique ({n} fichiers)")
                    except Exception as es:
                        saved.warning(f"⚠️ Sauvegarde échouée : {es}")

            link = data.get("@odata.nextLink")

    scan_err = None
    try:
        scan("root", "")
    except Exception as e:
        scan_err = e

    bar.progress(1.0)
    saved.empty()

    try:
        save_index(token, running)
        sidebar_stats.clear()
        elapsed = int(time.time() - t0)
        st.success(
            f"✅ Terminé en {elapsed}s — **{len(new_idx)} nouveaux** fichiers "
            f"(total : {len(running)}, ignorés : {skipped[0]})")
        if scan_err:
            st.warning(f"Scan interrompu : {scan_err}")
        st.balloons()
    except Exception as e:
        st.error(f"Sauvegarde finale échouée : {e}")
        st.download_button("⬇️ Télécharger l'index (JSON)",
            data=json.dumps(running, ensure_ascii=False, indent=2),
            file_name="file_index.json", mime="application/json")


# ─────────────────────────────────────────────────────────────
# RENDU
# ─────────────────────────────────────────────────────────────
with tab_search: tab_recherche()
with tab_browse: tab_parcourir()
with tab_index:  tab_indexation()
