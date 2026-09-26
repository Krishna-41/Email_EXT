#!/usr/bin/env python3
"""
Europe PMC Author Email Collector - production server (v4)

What this is
  A small Flask web app, backed by SQLite, that:
    - Lets any number of people open the site in a browser at the same time
    - Gives each visitor their OWN separate collected history (identified by
      a browser cookie, not a login) - your searches and your results, not
      mixed in with anyone else's
    - Lets you pick journals/publishers from a dropdown (loaded from
      journals.json, built from your spreadsheet) instead of typing links
    - Searches Europe PMC for any keyword(s) - "nano", "bio chemistry",
      "artificial intelligence", anything
    - Never re-collects an article you've already collected in an earlier
      run of the same (or overlapping) search
    - Lets you "watch" a search. The moment you do, it immediately collects
      every article that currently matches (so you're not just waiting for
      new ones) - and after that, a background thread re-checks every few
      hours and pulls in anything newly published, automatically.
    - Gives you two separate downloads: the emails from just your last run,
      and every email you've collected across all your runs.

Run locally
  pip install -r requirements.txt
  python app.py
  Open http://127.0.0.1:8000

Deploy
  See README_DEPLOY.md. Running "python app.py" already starts a production
  server (waitress) automatically if it's installed (it's in
  requirements.txt), so you won't see Flask's "development server" warning.
"""

import csv
import io
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from flask import Flask, jsonify, request, Response, g

# --------------------------------------------------------------------- config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("EPMC_DB_PATH", os.path.join(BASE_DIR, "collector.db"))
JOURNALS_FILE = os.path.join(BASE_DIR, "journals.json")
UPSTREAM = os.environ.get("EPMC_UPSTREAM", "https://www.ebi.ac.uk/europepmc/webservices/rest")
UA = "europepmc-author-email-collector/4.0"
REFRESH_INTERVAL_SECONDS = int(os.environ.get("EPMC_REFRESH_SECONDS", 6 * 3600))  # 6 hours
MAX_CONCURRENT_JOBS = int(os.environ.get("EPMC_MAX_JOBS", 2))
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
COOKIE_NAME = "epmc_uid"
COOKIE_MAX_AGE = 60 * 60 * 24 * 365 * 2  # ~2 years

app = Flask(__name__)

# ------------------------------------------------------------------- db setup
_db_lock = threading.RLock()  # SQLite + threads: serialize writes


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db():
    with _db_lock:
        conn = get_conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS articles (
                user_id TEXT,
                key TEXT,
                PRIMARY KEY (user_id, key)
            );
            CREATE TABLE IF NOT EXISTS emails (
                user_id TEXT,
                email TEXT,
                name TEXT,
                title TEXT,
                journal TEXT,
                year TEXT,
                doi TEXT,
                pmcid TEXT,
                term TEXT,
                job_id TEXT,
                added_at TEXT,
                PRIMARY KEY (user_id, email)
            );
            CREATE INDEX IF NOT EXISTS idx_emails_job ON emails(job_id);
            CREATE TABLE IF NOT EXISTS query_progress (
                user_id TEXT,
                qkey TEXT,
                cursor TEXT,
                scanned INTEGER,
                finished INTEGER,
                updated_at TEXT,
                PRIMARY KEY (user_id, qkey)
            );
            CREATE TABLE IF NOT EXISTS watches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                terms TEXT,        -- JSON list of search words
                journals TEXT,     -- JSON list of selected journal names
                mode TEXT,         -- all / phrase / any
                oa INTEGER,
                syn INTEGER,
                from_year INTEGER,
                to_year INTEGER,
                created_at TEXT,
                last_run_at TEXT,
                last_new_count INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_watches_user ON watches(user_id);
            """
        )
        conn.commit()
        conn.close()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------- per-user id
@app.before_request
def ensure_user():
    uid = request.cookies.get(COOKIE_NAME)
    if not uid:
        uid = uuid.uuid4().hex
        g.new_uid = uid
    g.user_id = uid


@app.after_request
def set_user_cookie(resp):
    new_uid = getattr(g, "new_uid", None)
    if new_uid:
        resp.set_cookie(COOKIE_NAME, new_uid, max_age=COOKIE_MAX_AGE, httponly=True, samesite="Lax")
    return resp


# ---------------------------------------------------------------- journals
def load_journals():
    if not os.path.exists(JOURNALS_FILE):
        return []
    with open(JOURNALS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


JOURNALS = load_journals()

# -------------------------------------------------------------- http helpers
def http_get_json(url, tries=4):
    last_err = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 8))
    raise RuntimeError(f"Could not reach Europe PMC: {last_err}")


def http_get_text(url, tries=3):
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=90) as r:
                return r.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
        except Exception:
            pass
        time.sleep(min(2 ** attempt, 6))
    return None


# ----------------------------------------------------------- email extraction
def clean_email(e):
    return e.strip().strip(".,;:()[]<>").lower()


def find_emails(text):
    return [clean_email(e) for e in EMAIL_RE.findall(text or "")]


def flat_text(el):
    return " ".join("".join(el.itertext()).split())


def unique(seq):
    seen, out = set(), []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def emails_in(el):
    tagged = [clean_email(e.text) for e in el.iter("email") if e.text]
    return unique(tagged + find_emails(flat_text(el)))


def authors_from_fulltext(xml_text):
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    meta = root.find(".//article-meta")
    if meta is None:
        return []
    corresp = {}
    for c in meta.iter("corresp"):
        if c.get("id"):
            corresp[c.get("id")] = emails_in(c)
    rows, assigned = [], set()
    for contrib in meta.iter("contrib"):
        if contrib.get("contrib-type") != "author":
            continue
        name_el = contrib.find("name")
        if name_el is not None:
            given = (name_el.findtext("given-names") or "").strip()
            sur = (name_el.findtext("surname") or "").strip()
            name = f"{given} {sur}".strip()
        else:
            collab = contrib.find("collab")
            name = flat_text(collab) if collab is not None else ""
        emails = emails_in(contrib)
        for xref in contrib.iter("xref"):
            if xref.get("ref-type") == "corresp":
                emails += corresp.get(xref.get("rid"), [])
        for e in unique(emails):
            rows.append((name, e))
            assigned.add(e)
    for e in emails_in(meta):
        if e not in assigned:
            rows.append(("(corresponding author - name not linked)", e))
    return rows


def authors_from_affiliations(record):
    rows = []
    authors = record.get("authorList", {}).get("author", []) or []
    for a in authors:
        texts = []
        if a.get("affiliation"):
            texts.append(a["affiliation"])
        for d in (a.get("authorAffiliationDetailsList", {}).get("authorAffiliation", []) or []):
            texts.append(d.get("affiliation", ""))
        for e in unique(find_emails(" ".join(texts))):
            rows.append((a.get("fullName", ""), e))
    return rows


# --------------------------------------------------------------- query build
def build_query(term, journals, mode, oa, from_year, to_year):
    if re.search(r"\b(AND|OR|NOT)\b|[\"():\[\]]", term):
        core = f"({term})"
    else:
        words = term.split()
        if len(words) == 1:
            core = words[0]
        elif mode == "phrase":
            core = f'"{term}"'
        else:
            joiner = " OR " if mode == "any" else " AND "
            core = "(" + joiner.join(words) + ")"
    q = f"{core} AND (FIRST_PDATE:[{from_year} TO {to_year}])"
    if journals:
        clauses = []
        for j in journals:
            j_clean = j.replace('"', "")
            clauses.append(f'JOURNAL:"{j_clean}" OR PUBLISHER:"{j_clean}"')
        q += " AND (" + " OR ".join(clauses) + ")"
    if oa:
        q += " AND OPEN_ACCESS:y"
    return q


def hit_count(query, synonym):
    url = f"{UPSTREAM}/search?query={urllib.parse.quote(query)}&format=json&resultType=lite&pageSize=1"
    if synonym:
        url += "&synonym=true"
    data = http_get_json(url)
    return data.get("hitCount", 0)


# --------------------------------------------------------------------- store
# Everything below is scoped per user_id, so two people running the same
# search each get their own copy of "have I seen this article/email before".
def article_keys(rec):
    keys = [f"{rec.get('source','')}:{rec.get('id','')}"]
    doi = rec.get("doi")
    if doi:
        keys.append("doi:" + doi.lower())
    return keys


def is_done(conn, user_id, rec):
    for k in article_keys(rec):
        row = conn.execute("SELECT 1 FROM articles WHERE user_id=? AND key=?", (user_id, k)).fetchone()
        if row:
            return True
    return False


def mark_done(conn, user_id, rec):
    for k in article_keys(rec):
        conn.execute("INSERT OR IGNORE INTO articles (user_id, key) VALUES (?,?)", (user_id, k))


def save_email(conn, user_id, name, email, rec, term, job_id):
    row = conn.execute("SELECT 1 FROM emails WHERE user_id=? AND email=?", (user_id, email)).fetchone()
    if row:
        return False
    conn.execute(
        "INSERT INTO emails (user_id, email, name, title, journal, year, doi, pmcid, term, job_id, added_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            user_id,
            email,
            name,
            (rec.get("title") or "").strip(),
            rec.get("journalInfo", {}).get("journal", {}).get("title", ""),
            rec.get("pubYear", ""),
            rec.get("doi", ""),
            rec.get("pmcid", ""),
            term,
            job_id,
            now_iso(),
        ),
    )
    return True


def get_progress(conn, user_id, qkey):
    row = conn.execute(
        "SELECT * FROM query_progress WHERE user_id=? AND qkey=?", (user_id, qkey)
    ).fetchone()
    return dict(row) if row else None


def save_progress(conn, user_id, qkey, cursor, scanned, finished):
    conn.execute(
        "INSERT INTO query_progress (user_id, qkey, cursor, scanned, finished, updated_at) VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(user_id, qkey) DO UPDATE SET cursor=excluded.cursor, scanned=excluded.scanned, "
        "finished=excluded.finished, updated_at=excluded.updated_at",
        (user_id, qkey, cursor, scanned, int(finished), now_iso()),
    )


# ------------------------------------------------------------------ job core
JOBS = {}  # job_id -> status dict (in-memory; lost on restart, DB data persists)
JOBS_LOCK = threading.Lock()
JOB_SEM = threading.Semaphore(MAX_CONCURRENT_JOBS)
_job_counter = [0]


def new_job_id():
    with JOBS_LOCK:
        _job_counter[0] += 1
        return f"job{_job_counter[0]}-{int(time.time())}"


def run_search(job_id, user_id, terms, journals, mode, oa, syn, from_year, to_year, max_per_term, stop_flag):
    """Core scan loop, shared by manual jobs, watch creation, and the auto-refresh scheduler."""
    total_new_articles = 0
    total_new_emails = 0
    per_term = []

    for term in terms:
        if stop_flag and stop_flag.is_set():
            break
        query = build_query(term, journals, mode, oa, from_year, to_year)
        qkey = query + ("|syn" if syn else "")

        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id]["status"] = f'"{term}": counting matches...'

        try:
            all_hits = hit_count(query, syn)
        except Exception as e:
            with JOBS_LOCK:
                if job_id in JOBS:
                    JOBS[job_id]["status"] = f"Error counting matches for '{term}': {e}"
            per_term.append({"term": term, "error": str(e)})
            continue

        conn = get_conn()
        prog = get_progress(conn, user_id, qkey)
        cursor = prog["cursor"] if prog and not prog["finished"] else "*"
        scanned = prog["scanned"] if prog and not prog["finished"] else 0
        new_art, new_em, skipped = 0, 0, 0

        while not (stop_flag and stop_flag.is_set()):
            if max_per_term and new_art >= max_per_term:
                break
            url = (
                f"{UPSTREAM}/search?query={urllib.parse.quote(query)}&format=json&resultType=core"
                f"&pageSize=100&cursorMark={urllib.parse.quote(cursor)}"
            )
            if syn:
                url += "&synonym=true"
            try:
                data = http_get_json(url, tries=(4 if cursor == "*" else 1))
            except Exception:
                if cursor != "*":
                    cursor, scanned = "*", 0
                    continue
                break
            results = data.get("resultList", {}).get("result", [])
            if not results:
                save_progress(conn, user_id, qkey, cursor, scanned, True)
                conn.commit()
                break

            for rec in results:
                if stop_flag and stop_flag.is_set():
                    break
                if max_per_term and new_art >= max_per_term:
                    break
                if is_done(conn, user_id, rec):
                    skipped += 1
                    continue
                rows = []
                if rec.get("pmcid") and rec.get("isOpenAccess") == "Y":
                    xml_text = http_get_text(f"{UPSTREAM}/{rec['pmcid']}/fullTextXML")
                    if xml_text:
                        try:
                            rows += authors_from_fulltext(xml_text)
                        except Exception:
                            pass
                rows += authors_from_affiliations(rec)
                for name, email in rows:
                    if save_email(conn, user_id, name, email, rec, term, job_id):
                        new_em += 1
                mark_done(conn, user_id, rec)
                new_art += 1
                conn.commit()
                with JOBS_LOCK:
                    if job_id in JOBS:
                        JOBS[job_id]["status"] = (
                            f'"{term}": {new_art} new articles processed | '
                            f"{new_em} new emails | {skipped} already-collected skipped"
                        )

            nxt = data.get("nextCursorMark")
            scanned += len(results)
            if not nxt or nxt == cursor or (max_per_term and new_art >= max_per_term):
                finished = not nxt or nxt == cursor
                save_progress(conn, user_id, qkey, cursor if not nxt else nxt, scanned, finished)
                conn.commit()
                break
            cursor = nxt
            save_progress(conn, user_id, qkey, cursor, scanned, False)
            conn.commit()

        conn.close()
        total_new_articles += new_art
        total_new_emails += new_em
        per_term.append({"term": term, "all_hits": all_hits, "new_articles": new_art,
                          "new_emails": new_em, "skipped": skipped})

    return {"new_articles": total_new_articles, "new_emails": total_new_emails, "per_term": per_term}


def job_worker(job_id, params):
    with JOBS_LOCK:
        JOBS[job_id]["state"] = "running"
    stop_flag = JOBS[job_id]["stop_flag"]
    try:
        with JOB_SEM:
            result = run_search(job_id=job_id, stop_flag=stop_flag, **params)
        with JOBS_LOCK:
            JOBS[job_id]["state"] = "stopped" if stop_flag.is_set() else "done"
            JOBS[job_id]["result"] = result
            JOBS[job_id]["status"] = (
                f"Finished. {result['new_articles']} new articles, {result['new_emails']} new emails."
            )
    except Exception as e:
        with JOBS_LOCK:
            JOBS[job_id]["state"] = "error"
            JOBS[job_id]["status"] = f"Error: {e}"


def start_job(user_id, terms, journals, mode, oa, syn, from_year, to_year, max_per_term, watch_id=None):
    """Common helper: register a job and run it in a background thread. Returns job_id."""
    job_id = new_job_id()
    stop_flag = threading.Event()
    with JOBS_LOCK:
        JOBS[job_id] = {
            "state": "queued", "status": "Queued...", "stop_flag": stop_flag,
            "user_id": user_id, "watch_id": watch_id,
        }
    params = dict(
        user_id=user_id, terms=terms, journals=journals, mode=mode, oa=oa, syn=syn,
        from_year=from_year, to_year=to_year, max_per_term=max_per_term,
    )
    t = threading.Thread(target=job_worker, args=(job_id, params), daemon=True)
    t.start()
    return job_id


# ----------------------------------------------------------------- watches
def run_watch_now(w, existing_job_id=None):
    """Run a watch's search immediately (used both right after creating a watch,
    and by the scheduler). Updates the watch's last_run_at / last_new_count."""
    terms = json.loads(w["terms"])
    journals = json.loads(w["journals"])
    job_id = existing_job_id or new_job_id()
    stop_flag = threading.Event()
    with JOBS_LOCK:
        JOBS.setdefault(job_id, {})
        JOBS[job_id].update({
            "state": "running", "status": "Collecting matching articles...",
            "stop_flag": stop_flag, "watch_id": w["id"], "user_id": w["user_id"],
        })
    result = run_search(
        job_id=job_id, user_id=w["user_id"], terms=terms, journals=journals, mode=w["mode"],
        oa=bool(w["oa"]), syn=bool(w["syn"]), from_year=w["from_year"], to_year=w["to_year"],
        max_per_term=0, stop_flag=stop_flag,
    )
    conn = get_conn()
    conn.execute(
        "UPDATE watches SET last_run_at=?, last_new_count=? WHERE id=?",
        (now_iso(), result["new_emails"], w["id"]),
    )
    conn.commit()
    conn.close()
    with JOBS_LOCK:
        JOBS[job_id]["state"] = "done"
        JOBS[job_id]["result"] = result
        JOBS[job_id]["status"] = (
            f"Finished. {result['new_articles']} articles processed, {result['new_emails']} new emails."
        )
    return job_id


def scheduler_loop():
    while True:
        time.sleep(REFRESH_INTERVAL_SECONDS)
        try:
            conn = get_conn()
            watches = conn.execute("SELECT * FROM watches").fetchall()
            conn.close()
            for w in watches:
                try:
                    run_watch_now(w)
                except Exception as e:
                    print(f"Scheduler error on watch {w['id']}:", e)
        except Exception as e:
            print("Scheduler error:", e)


# ------------------------------------------------------------------- routes
@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


@app.route("/api/journals")
def api_journals():
    return jsonify(JOURNALS)


@app.route("/api/hitcount")
def api_hitcount():
    term = request.args.get("term", "")
    journals = request.args.getlist("journal")
    mode = request.args.get("mode", "all")
    oa = request.args.get("oa") == "1"
    syn = request.args.get("syn") == "1"
    from_year = request.args.get("from", "2024")
    to_year = request.args.get("to", "2026")
    if not term:
        return jsonify({"error": "missing term"}), 400
    q = build_query(term, journals, mode, oa, from_year, to_year)
    try:
        n = hit_count(q, syn)
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"term": term, "hitCount": n, "query": q})


@app.route("/api/run", methods=["POST"])
def api_run():
    body = request.get_json(force=True)
    terms = [t.strip() for t in body.get("terms", []) if t.strip()]
    if not terms:
        return jsonify({"error": "no search words given"}), 400
    job_id = start_job(
        user_id=g.user_id,
        terms=terms,
        journals=body.get("journals", []),
        mode=body.get("mode", "all"),
        oa=bool(body.get("oa", True)),
        syn=bool(body.get("syn", True)),
        from_year=int(body.get("from", 2024)),
        to_year=int(body.get("to", 2026)),
        max_per_term=int(body.get("max", 200)),
    )
    return jsonify({"job_id": job_id})


@app.route("/api/job/<job_id>")
def api_job(job_id):
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        if not j:
            return jsonify({"error": "unknown job"}), 404
        return jsonify({"state": j["state"], "status": j["status"], "result": j.get("result")})


@app.route("/api/job/<job_id>/stop", methods=["POST"])
def api_job_stop(job_id):
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        if not j:
            return jsonify({"error": "unknown job"}), 404
        j["stop_flag"].set()
    return jsonify({"ok": True})


@app.route("/api/stats")
def api_stats():
    conn = get_conn()
    n_articles = conn.execute(
        "SELECT COUNT(*) c FROM articles WHERE user_id=?", (g.user_id,)
    ).fetchone()["c"]
    n_emails = conn.execute(
        "SELECT COUNT(*) c FROM emails WHERE user_id=?", (g.user_id,)
    ).fetchone()["c"]
    conn.close()
    return jsonify({"articles": n_articles, "emails": n_emails})


@app.route("/api/emails")
def api_emails():
    limit = min(int(request.args.get("limit", 200)), 2000)
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM emails WHERE user_id=? ORDER BY added_at DESC LIMIT ?", (g.user_id, limit)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


def _emails_csv(rows, filename):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["author_name", "email", "article_title", "journal", "year", "doi", "pmcid", "search_word", "added_at"])
    for r in rows:
        w.writerow([r["name"], r["email"], r["title"], r["journal"], r["year"], r["doi"], r["pmcid"], r["term"], r["added_at"]])
    data = "\ufeff" + buf.getvalue()
    return Response(
        data, mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/api/download.csv")
def api_download_all():
    """Every email this user (this browser) has ever collected, across all runs."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM emails WHERE user_id=? ORDER BY added_at", (g.user_id,)
    ).fetchall()
    conn.close()
    return _emails_csv(rows, "authors_emails_ALL.csv")


@app.route("/api/download/job/<job_id>.csv")
def api_download_job(job_id):
    """Only the emails that THIS specific run added (new emails only - an
    email already known from an earlier run doesn't get re-counted here)."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM emails WHERE user_id=? AND job_id=? ORDER BY added_at", (g.user_id, job_id)
    ).fetchall()
    conn.close()
    return _emails_csv(rows, f"authors_emails_{job_id}.csv")


@app.route("/api/watches", methods=["GET", "POST"])
def api_watches():
    user_id = g.user_id
    conn = get_conn()
    if request.method == "POST":
        body = request.get_json(force=True)
        terms = [t.strip() for t in body.get("terms", []) if t.strip()]
        if not terms:
            conn.close()
            return jsonify({"error": "no search words given"}), 400
        conn.execute(
            "INSERT INTO watches (user_id, terms, journals, mode, oa, syn, from_year, to_year, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                user_id,
                json.dumps(terms),
                json.dumps(body.get("journals", [])),
                body.get("mode", "all"),
                int(bool(body.get("oa", True))),
                int(bool(body.get("syn", True))),
                int(body.get("from", 2024)),
                int(body.get("to", 2026)),
                now_iso(),
            ),
        )
        conn.commit()
        watch_id = conn.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        w = conn.execute("SELECT * FROM watches WHERE id=?", (watch_id,)).fetchone()
        conn.close()

        # Immediately collect everything that currently matches - don't make
        # the person wait for the next scheduled refresh to see anything.
        job_id = new_job_id()
        stop_flag = threading.Event()
        with JOBS_LOCK:
            JOBS[job_id] = {
                "state": "queued", "status": "Collecting every currently matching article...",
                "stop_flag": stop_flag, "watch_id": watch_id, "user_id": user_id,
            }
        threading.Thread(target=lambda: run_watch_now(dict(w), existing_job_id=job_id), daemon=True).start()
        return jsonify({"id": watch_id, "job_id": job_id})

    rows = conn.execute("SELECT * FROM watches WHERE user_id=? ORDER BY id DESC", (user_id,)).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        d["terms"] = json.loads(d["terms"])
        d["journals"] = json.loads(d["journals"])
        out.append(d)
    return jsonify(out)


@app.route("/api/watches/<int:watch_id>", methods=["DELETE"])
def api_watch_delete(watch_id):
    conn = get_conn()
    conn.execute("DELETE FROM watches WHERE id=? AND user_id=?", (watch_id, g.user_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------- page
PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Europe PMC Author Email Collector</title>
<style>
  :root { color-scheme: light dark; --bd:#8884; --ac:#2a6fdb; }
  body { font-family: system-ui, Segoe UI, Arial, sans-serif; max-width: 1050px; margin: 24px auto; padding: 0 16px; }
  h1 { font-size: 1.4rem; margin: 0 0 4px; }
  p.sub { margin: 0 0 8px; opacity: .7; font-size: .9rem; }
  #stats { font-size: .85rem; padding: 8px 10px; border: 1px solid var(--bd); border-radius: 6px; margin-bottom: 8px; }
  label { display: block; font-weight: 600; font-size: .85rem; margin: 12px 0 4px; }
  textarea, input[type=number], input[type=text], select { width: 100%; box-sizing: border-box; padding: 8px; border: 1px solid var(--bd); border-radius: 6px; font: inherit; background: transparent; color: inherit; }
  textarea { height: 90px; }
  .row { display: flex; gap: 12px; flex-wrap: wrap; }
  .row > div { flex: 1 1 140px; }
  .chk { font-weight: 400; display: flex; gap: 8px; align-items: center; margin-top: 10px; }
  .hint { font-weight: 400; opacity: .65; font-size: .78rem; }
  button { padding: 9px 16px; border: 0; border-radius: 6px; font: inherit; font-weight: 600; cursor: pointer; background: var(--ac); color: #fff; margin: 16px 8px 0 0; }
  button.sec { background: transparent; color: inherit; border: 1px solid var(--bd); }
  button:disabled { opacity: .4; cursor: default; }
  #status { margin: 14px 0 6px; font-size: .9rem; min-height: 1.3em; }
  .tw { overflow-x: auto; border: 1px solid var(--bd); border-radius: 6px; max-height: 340px; overflow-y: auto; margin-bottom: 12px; }
  table { border-collapse: collapse; width: 100%; font-size: .82rem; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid var(--bd); white-space: nowrap; max-width: 320px; overflow: hidden; text-overflow: ellipsis; }
  th { position: sticky; top: 0; background: Canvas; }
  h2 { font-size: 1rem; margin: 18px 0 6px; }
  #journalBox { border: 1px solid var(--bd); border-radius: 6px; max-height: 220px; overflow-y: auto; padding: 6px 8px; }
  #journalBox label { font-weight: 400; margin: 2px 0; display: flex; gap: 6px; align-items: center; }
  .catHead { font-weight: 700; margin-top: 8px; opacity: .8; }
</style>
</head>
<body>
<h1>Europe PMC Author Email Collector</h1>
<p class="sub">Your own collected data on this browser - not shared with other visitors.</p>
<div id="stats">Loading your stats...</div>

<label for="terms">Search words (one per line) <span class="hint">- e.g. nano materials, bio chemistry, artificial intelligence</span></label>
<textarea id="terms">nano</textarea>

<label for="journalFilter">Journals / publishers (optional - pick from your list, or leave empty for all)</label>
<input type="text" id="journalFilter" placeholder="Type to filter the list below...">
<div id="journalBox"></div>

<div class="row">
  <div><label for="mode">Multi-word matching</label>
    <select id="mode">
      <option value="all">All words anywhere (bio AND chemistry)</option>
      <option value="phrase">Exact phrase ("bio chemistry")</option>
      <option value="any">Any word (bio OR chemistry)</option>
    </select></div>
  <div><label for="from">From year</label><input type="number" id="from" value="2024"></div>
  <div><label for="to">To year</label><input type="number" id="to" value="2026"></div>
  <div><label for="max">Max NEW articles per search word (0 = all remaining)</label><input type="number" id="max" value="200" min="0"></div>
</div>

<label class="chk"><input type="checkbox" id="oa" checked> Open-access articles only (full text available, far more emails)</label>
<label class="chk"><input type="checkbox" id="syn" checked> Include synonyms</label>

<button id="go">Start</button>
<button id="stop" class="sec" disabled>Stop</button>
<button id="watchBtn" class="sec">Auto-refresh this search (collects everything now, then keeps checking for new articles)</button>
<br>
<button id="dlRun" class="sec" disabled>Download this run (CSV)</button>
<button id="dlAll" class="sec">Download all my collected data (CSV)</button>

<div id="status"></div>

<h2>My watched searches (auto-updated in the background)</h2>
<div class="tw"><table>
  <thead><tr><th>Search words</th><th>Journals</th><th>Last run</th><th>New last run</th><th></th></tr></thead>
  <tbody id="wb"></tbody>
</table></div>

<h2>My most recently collected emails</h2>
<div class="tw"><table>
  <thead><tr><th>Author</th><th>Email</th><th>Article</th><th>Search word</th><th>Added</th></tr></thead>
  <tbody id="tb"></tbody>
</table></div>

<script>
const $ = id => document.getElementById(id);
let JOURNALS = [], currentJob = null, poll = null;

async function loadJournals() {
  JOURNALS = await (await fetch("/api/journals")).json();
  renderJournals();
}
function renderJournals(filterText) {
  const box = $("journalBox"); box.innerHTML = "";
  const f = (filterText || "").toLowerCase();
  let lastCat = null;
  JOURNALS.filter(j => !f || j.name.toLowerCase().includes(f)).forEach(j => {
    if (j.category !== lastCat) { const h = document.createElement("div"); h.className = "catHead"; h.textContent = j.category; box.appendChild(h); lastCat = j.category; }
    const lab = document.createElement("label");
    const cb = document.createElement("input"); cb.type = "checkbox"; cb.value = j.name; cb.dataset.journal = "1";
    lab.appendChild(cb); lab.appendChild(document.createTextNode(j.name));
    box.appendChild(lab);
  });
}
$("journalFilter").oninput = e => renderJournals(e.target.value);
const selectedJournals = () => [...document.querySelectorAll('#journalBox input[type=checkbox]:checked')].map(c => c.value);

async function loadStats() {
  const s = await (await fetch("/api/stats")).json();
  $("stats").textContent = `Your history on this browser: ${s.articles.toLocaleString()} articles processed, ${s.emails.toLocaleString()} unique emails collected so far.`;
}
async function loadRecent() {
  const rows = await (await fetch("/api/emails?limit=300")).json();
  $("tb").innerHTML = "";
  rows.forEach(r => {
    const tr = document.createElement("tr");
    [r.name, r.email, r.title, r.term, (r.added_at || "").replace("T"," ").slice(0,16)].forEach(v => {
      const td = document.createElement("td"); td.textContent = v; td.title = v; tr.appendChild(td);
    });
    $("tb").appendChild(tr);
  });
}
async function loadWatches() {
  const rows = await (await fetch("/api/watches")).json();
  $("wb").innerHTML = "";
  rows.forEach(w => {
    const tr = document.createElement("tr");
    const cells = [w.terms.join(", "), (w.journals.length ? w.journals.join(", ") : "(all)"),
                   (w.last_run_at || "never").replace("T"," ").slice(0,16), w.last_new_count ?? 0];
    cells.forEach(v => { const td = document.createElement("td"); td.textContent = v; tr.appendChild(td); });
    const td = document.createElement("td");
    const b = document.createElement("button"); b.className = "sec"; b.textContent = "Remove"; b.style.margin = "0";
    b.onclick = async () => { await fetch("/api/watches/" + w.id, { method: "DELETE" }); loadWatches(); };
    td.appendChild(b); tr.appendChild(td);
    $("wb").appendChild(tr);
  });
}

function params() {
  return {
    terms: $("terms").value.split("\n").map(s => s.trim()).filter(Boolean),
    journals: selectedJournals(),
    mode: $("mode").value,
    oa: $("oa").checked,
    syn: $("syn").checked,
    from: parseInt($("from").value) || 2024,
    to: parseInt($("to").value) || 2026,
    max: parseInt($("max").value) || 0,
  };
}

function beginPolling(job_id) {
  currentJob = job_id;
  $("go").disabled = true; $("stop").disabled = false; $("dlRun").disabled = true;
  poll = setInterval(checkJob, 1500);
}

async function start() {
  const p = params();
  if (!p.terms.length) { $("status").textContent = "Please enter at least one search word."; return; }
  const r = await fetch("/api/run", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) });
  const d = await r.json();
  if (d.error) { $("status").textContent = "Error: " + d.error; return; }
  beginPolling(d.job_id);
}
async function checkJob() {
  if (!currentJob) return;
  const d = await (await fetch("/api/job/" + currentJob)).json();
  $("status").textContent = d.status || "";
  if (d.state === "done" || d.state === "stopped" || d.state === "error") {
    clearInterval(poll); $("go").disabled = false; $("stop").disabled = true;
    if (d.state === "done") $("dlRun").disabled = false;
    loadStats(); loadRecent(); loadWatches();
  }
}
async function stop() {
  if (currentJob) await fetch("/api/job/" + currentJob + "/stop", { method: "POST" });
}
async function watchThis() {
  const p = params();
  if (!p.terms.length) { $("status").textContent = "Please enter at least one search word."; return; }
  const r = await fetch("/api/watches", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) });
  const d = await r.json();
  if (d.error) { $("status").textContent = "Error: " + d.error; return; }
  loadWatches();
  $("status").textContent = "Saved. Collecting every currently matching article now...";
  if (d.job_id) beginPolling(d.job_id);
}

$("go").onclick = start;
$("stop").onclick = stop;
$("watchBtn").onclick = watchThis;
$("dlRun").onclick = () => { if (currentJob) location.href = "/api/download/job/" + currentJob + ".csv"; };
$("dlAll").onclick = () => { location.href = "/api/download.csv"; };

loadJournals(); loadStats(); loadRecent(); loadWatches();
setInterval(loadStats, 30000);
</script>
</body>
</html>
"""

init_db()

if os.environ.get("EPMC_RUN_SCHEDULER", "1") == "1":
    threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    try:
        from waitress import serve as _serve
        print(f"Starting production server (waitress) on http://0.0.0.0:{port}")
        print("(No 'development server' warning - this is a real production server.)")
        _serve(app, host="0.0.0.0", port=port)
    except ImportError:
        print("NOTE: 'waitress' isn't installed, so this is using Flask's development")
        print("server, which will print its own warning below. Run:")
        print("    pip install waitress")
        print("and start this again to use a real production server instead.\n")
        app.run(host="0.0.0.0", port=port, threaded=True)
