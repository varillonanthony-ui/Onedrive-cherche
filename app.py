import streamlit as st
import sqlite3
import json
import time
from datetime import datetime
from onedrive_client import OneDriveClient
from indexer import OneDriveIndexer
from search import SearchEngine

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OneDrive Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Sora:wght@300;400;600;700&display=swap');

:root {
    --bg: #0f0f13;
    --surface: #16161d;
    --border: #2a2a38;
    --accent: #6c63ff;
    --accent2: #ff6b9d;
    --text: #e8e8f0;
    --muted: #6b6b80;
    --success: #4ade80;
    --warning: #fbbf24;
    --error: #f87171;
}

html, body, [data-testid="stApp"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'Sora', sans-serif !important;
}

[data-testid="stSidebar"] {
    background-color: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}

.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background-color: var(--surface) !important;
    color: var(--text) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    font-family: 'JetBrains Mono', monospace !important;
}

.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 2px rgba(108,99,255,0.2) !important;
}

.stButton > button {
    background: linear-gradient(135deg, var(--accent), var(--accent2)) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.5rem !important;
    transition: opacity 0.2s, transform 0.1s !important;
}
.stButton > button:hover {
    opacity: 0.88 !important;
    transform: translateY(-1px) !important;
}

.result-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1rem;
    transition: border-color 0.2s;
}
.result-card:hover { border-color: var(--accent); }

.result-title {
    font-weight: 600;
    font-size: 1rem;
    color: var(--accent);
    margin-bottom: 0.3rem;
}
.result-path {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: var(--muted);
    margin-bottom: 0.5rem;
    word-break: break-all;
}
.result-snippet {
    font-size: 0.85rem;
    color: #b0b0c0;
    line-height: 1.6;
    border-left: 3px solid var(--accent);
    padding-left: 0.8rem;
    margin-top: 0.5rem;
}
.result-meta {
    display: flex;
    gap: 1rem;
    margin-top: 0.6rem;
    flex-wrap: wrap;
}
.badge {
    font-size: 0.7rem;
    font-family: 'JetBrains Mono', monospace;
    padding: 2px 8px;
    border-radius: 20px;
    background: rgba(108,99,255,0.15);
    color: var(--accent);
    border: 1px solid rgba(108,99,255,0.3);
}
.badge-name { background: rgba(74,222,128,0.1); color: var(--success); border-color: rgba(74,222,128,0.3); }
.badge-content { background: rgba(255,107,157,0.1); color: var(--accent2); border-color: rgba(255,107,157,0.3); }

.stat-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1rem;
    text-align: center;
}
.stat-num { font-size: 1.8rem; font-weight: 700; color: var(--accent); }
.stat-label { font-size: 0.75rem; color: var(--muted); margin-top: 0.2rem; }

.highlight { background: rgba(108,99,255,0.25); border-radius: 3px; padding: 0 2px; color: var(--text); }

h1,h2,h3 { font-family: 'Sora', sans-serif !important; }

[data-testid="stProgress"] > div > div {
    background: linear-gradient(135deg, var(--accent), var(--accent2)) !important;
}

.stSelectbox > div > div { background: var(--surface) !important; color: var(--text) !important; border-color: var(--border) !important; }
.stCheckbox > label { color: var(--text) !important; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "client" not in st.session_state:
    st.session_state.client = None
if "indexing" not in st.session_state:
    st.session_state.indexing = False
if "index_stats" not in st.session_state:
    st.session_state.index_stats = {}

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_file_icon(ext: str) -> str:
    icons = {
        "pdf": "📄", "docx": "📝", "doc": "📝",
        "xlsx": "📊", "xls": "📊", "csv": "📊",
        "pptx": "📑", "ppt": "📑",
        "txt": "📃", "md": "📃",
        "png": "🖼️", "jpg": "🖼️", "jpeg": "🖼️",
        "mp4": "🎬", "mp3": "🎵",
        "zip": "📦", "py": "🐍", "js": "⚙️",
    }
    return icons.get(ext.lower().lstrip("."), "📁")

def highlight_text(text: str, query: str, max_len: int = 300) -> str:
    if not text or not query:
        return text[:max_len] + "..." if text and len(text) > max_len else (text or "")
    lower_text = text.lower()
    lower_query = query.lower()
    idx = lower_text.find(lower_query)
    if idx == -1:
        return text[:max_len] + "..." if len(text) > max_len else text
    start = max(0, idx - 80)
    end = min(len(text), idx + len(query) + 150)
    snippet = ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")
    highlighted = snippet.replace(
        text[idx:idx+len(query)],
        f'<span class="highlight">{text[idx:idx+len(query)]}</span>',
        1
    )
    return highlighted

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown("---")

    st.markdown("### 🔐 Authentification Microsoft")
    client_id = st.text_input("Client ID (Azure App)", type="password",
                               help="App ID de votre application Azure AD")
    client_secret = st.text_input("Client Secret", type="password")
    tenant_id = st.text_input("Tenant ID", value="common",
                               help="'common' pour un compte perso, ou votre Tenant ID pro")

    st.markdown("---")
    st.markdown("### 🗄️ Base de données locale")
    db_path = st.text_input("Chemin SQLite", value="onedrive_index.db")

    st.markdown("---")
    st.markdown("### 📂 Filtres d'indexation")
    max_file_size = st.slider("Taille max fichier (Mo)", 1, 100, 20)
    index_content = st.checkbox("Indexer le contenu des fichiers", value=True)

    st.markdown("---")
    if st.button("🔗 Se connecter à OneDrive"):
        if not client_id or not client_secret:
            st.error("Renseignez Client ID et Secret.")
        else:
            with st.spinner("Connexion..."):
                try:
                    c = OneDriveClient(client_id, client_secret, tenant_id)
                    token = c.authenticate_device_flow()
                    if token:
                        st.session_state.client = c
                        st.success("✅ Connecté !")
                        info = c.get_user_info()
                        st.info(f"👤 {info.get('displayName','Utilisateur')} — {info.get('mail','')}")
                except Exception as e:
                    st.error(f"Erreur : {e}")

    status = "🟢 Connecté" if st.session_state.client else "🔴 Non connecté"
    st.markdown(f"**Statut :** {status}")

# ── Main ──────────────────────────────────────────────────────────────────────
st.markdown("# 🔍 OneDrive Search")
st.markdown("Indexez et recherchez dans tous vos fichiers OneDrive.")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["🔎 Recherche", "⚡ Indexation", "📊 Statistiques"])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SEARCH
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    col1, col2 = st.columns([4, 1])
    with col1:
        query = st.text_input("", placeholder="🔍 Rechercher un mot-clé dans vos fichiers...",
                               label_visibility="collapsed")
    with col2:
        search_btn = st.button("Rechercher", use_container_width=True)

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        search_type = st.selectbox("Rechercher dans", ["Nom + Contenu", "Nom du fichier", "Contenu uniquement"])
    with col_b:
        ext_filter = st.selectbox("Type de fichier", ["Tous", "PDF", "Word (.docx)", "Excel (.xlsx)", "PowerPoint", "Texte"])
    with col_c:
        max_results = st.selectbox("Résultats max", [20, 50, 100, 200], index=0)

    ext_map = {
        "Tous": None, "PDF": ["pdf"], "Word (.docx)": ["docx","doc"],
        "Excel (.xlsx)": ["xlsx","xls","csv"], "PowerPoint": ["pptx","ppt"],
        "Texte": ["txt","md"],
    }

    if (search_btn or query) and query.strip():
        try:
            engine = SearchEngine(db_path)
            mode = {"Nom + Contenu": "both", "Nom du fichier": "name", "Contenu uniquement": "content"}[search_type]
            exts = ext_map[ext_filter]

            with st.spinner("Recherche en cours..."):
                results = engine.search(query.strip(), mode=mode, extensions=exts, limit=max_results)

            if not results:
                st.warning("Aucun résultat trouvé. Vérifiez que l'index est à jour (onglet Indexation).")
            else:
                st.markdown(f"**{len(results)} résultat(s)** pour *{query}*")
                st.markdown("")

                for r in results:
                    ext = r.get("extension", "").lstrip(".")
                    icon = get_file_icon(ext)
                    match_type = r.get("match_type", "name")
                    badge_class = "badge-name" if match_type == "name" else "badge-content"
                    badge_label = "📛 Nom" if match_type == "name" else "📖 Contenu"

                    snippet_html = ""
                    if r.get("content_snippet"):
                        highlighted = highlight_text(r["content_snippet"], query)
                        snippet_html = f'<div class="result-snippet">{highlighted}</div>'

                    size_str = f"{r.get('size_bytes',0)/1024:.0f} Ko" if r.get('size_bytes') else ""
                    date_str = r.get("modified_at", "")[:10] if r.get("modified_at") else ""

                    card = f"""
<div class="result-card">
  <div class="result-title">{icon} {r.get('name','')}</div>
  <div class="result-path">{r.get('path','')}</div>
  {snippet_html}
  <div class="result-meta">
    <span class="badge {badge_class}">{badge_label}</span>
    <span class="badge">.{ext.upper()}</span>
    {'<span class="badge">'+size_str+'</span>' if size_str else ''}
    {'<span class="badge">'+date_str+'</span>' if date_str else ''}
  </div>
</div>"""
                    st.markdown(card, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Erreur lors de la recherche : {e}")
            st.info("Assurez-vous d'avoir lancé une indexation au préalable.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — INDEXATION
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### ⚡ Indexer votre OneDrive")

    col1, col2 = st.columns(2)
    with col1:
        start_path = st.text_input("Dossier de départ", value="/",
                                    help="'/' pour tout indexer, ou '/Documents' pour un sous-dossier")
    with col2:
        workers = st.slider("Threads parallèles", 1, 8, 4)

    col3, col4 = st.columns(2)
    with col3:
        btn_full = st.button("🚀 Indexation complète", use_container_width=True,
                              disabled=not st.session_state.client)
    with col4:
        btn_update = st.button("🔄 Mise à jour (delta)", use_container_width=True,
                                disabled=not st.session_state.client)

    if not st.session_state.client:
        st.info("👈 Connectez-vous à OneDrive dans la barre latérale pour lancer l'indexation.")

    if btn_full or btn_update:
        st.session_state.indexing = True
        delta_only = btn_update

        progress_bar = st.progress(0)
        status_text = st.empty()
        stats_placeholder = st.empty()

        try:
            indexer = OneDriveIndexer(
                client=st.session_state.client,
                db_path=db_path,
                index_content=index_content,
                max_file_size_mb=max_file_size,
                num_workers=workers,
            )

            total_files = [0]
            indexed_files = [0]
            errors = [0]

            def progress_callback(current, total, filename, error=False):
                if total > 0:
                    pct = min(current / total, 1.0)
                    progress_bar.progress(pct)
                if error:
                    errors[0] += 1
                status_text.markdown(
                    f"⏳ `{current}/{total}` — `{filename[:60]}`  |  "
                    f"✅ `{current - errors[0]}` OK  |  ❌ `{errors[0]}` erreurs"
                )

            start_time = time.time()
            stats = indexer.run(start_path=start_path, delta=delta_only, callback=progress_callback)
            elapsed = time.time() - start_time

            progress_bar.progress(1.0)
            st.session_state.index_stats = stats
            st.success(f"✅ Indexation terminée en {elapsed:.1f}s — {stats.get('indexed',0)} fichiers indexés.")
            st.balloons()

        except Exception as e:
            st.error(f"Erreur d'indexation : {e}")
        finally:
            st.session_state.indexing = False

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — STATS
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### 📊 Statistiques de l'index")

    try:
        engine = SearchEngine(db_path)
        stats = engine.get_stats()

        c1, c2, c3, c4 = st.columns(4)
        metrics = [
            ("Fichiers indexés", stats.get("total_files", 0), c1),
            ("Avec contenu extrait", stats.get("with_content", 0), c2),
            ("Taille totale", f"{stats.get('total_size_mb', 0):.1f} Mo", c3),
            ("Dernière indexation", stats.get("last_indexed", "—")[:10] if stats.get("last_indexed") else "—", c4),
        ]
        for label, val, col in metrics:
            with col:
                st.markdown(f"""
<div class="stat-box">
  <div class="stat-num">{val}</div>
  <div class="stat-label">{label}</div>
</div>""", unsafe_allow_html=True)

        st.markdown("")
        st.markdown("#### Répartition par type de fichier")
        ext_stats = stats.get("by_extension", {})
        if ext_stats:
            sorted_exts = sorted(ext_stats.items(), key=lambda x: x[1], reverse=True)[:15]
            labels = [f"{get_file_icon(e)} .{e.upper()}" for e, _ in sorted_exts]
            values = [v for _, v in sorted_exts]

            chart_data = {"Type": labels, "Fichiers": values}
            import pandas as pd
            df = pd.DataFrame(chart_data)
            st.bar_chart(df.set_index("Type"))
        else:
            st.info("Aucune donnée — lancez d'abord une indexation.")

    except Exception:
        st.info("Aucune base de données trouvée. Lancez une indexation pour commencer.")
