#!/usr/bin/env python3
"""
Europe PMC Author Email Collector - production server (v5)

What this is
  A small Flask web app, backed by SQLite, that:
    - Lets any number of people open the site in a browser at the same time
    - Gives each visitor their OWN separate collected history (identified by
      a browser cookie, not a login) - your searches and your results, not
      mixed in with anyone else's
    - Lets you pick journals/publishers from a dropdown (loaded from
      journals.json, built from your spreadsheet) instead of typing links
    - Searches Europe PMC for any keyword(s) - "nano", "bio chemistry",
      "artificial intelligence", anything - across all years by default;
      an optional date range narrows it down further, only if you set one
    - Shows the total match count for each search word automatically as you
      type, before you commit to collecting anything
    - Never re-collects an article you've already collected in an earlier
      run of the same (or overlapping) search
    - Lets you download the emails found by your most recent run

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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
UA = "europepmc-author-email-collector/5.0"
MAX_CONCURRENT_JOBS = int(os.environ.get("EPMC_MAX_JOBS", 2))
# Fetching each article's full-text XML is the slow part (one network round
# trip per article). Doing them concurrently instead of one-at-a-time is
# what makes large runs fast - tune down if Europe PMC ever rate-limits you.
FULLTEXT_WORKERS = int(os.environ.get("EPMC_FULLTEXT_WORKERS", 10))
# Articles are fetched in chunks this big, not all-at-once, so that clicking
# Pause takes effect within one chunk instead of waiting for an entire
# 100-article page to finish.
CHUNK_SIZE = FULLTEXT_WORKERS * 2
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,24}")
# Top-level domains/second-level combos common in academic emails. Used only to
# clean up an email that got a stray word glued onto the end (see
# strip_glued_suffix) - not used to reject emails with newer/uncommon TLDs.
COMMON_TLDS = {
    "com", "org", "net", "edu", "gov", "mil", "int", "info", "biz", "io", "ai",
    "co", "in", "uk", "us", "cn", "jp", "de", "fr", "it", "es", "br", "au", "ca",
    "kr", "ru", "nl", "se", "ch", "at", "be", "dk", "no", "fi", "pl", "gr", "pt",
    "tr", "sg", "hk", "tw", "nz", "mx", "il", "eg", "pk", "my", "th", "id", "ng",
    "ke", "za", "ir", "sa", "ae",
}
# Words that sometimes end up glued directly onto an email with no space,
# when the source text has no whitespace between two words at all (rare, but
# happens in some publishers' XML/PDF-derived text).
_GLUE_WORDS = sorted(
    ["for", "and", "the", "author", "authors", "corresponding", "email",
     "contact", "address", "tel", "fax", "phone", "received", "accepted",
     "published", "keywords", "abstract", "introduction", "department",
     "university", "school", "institute", "college"],
    key=len, reverse=True,
)


def strip_glued_suffix(email):
    """If a common English word is glued onto a real TLD with no separator
    (e.g. 'uni.educontact' from 'uni.edu' + 'Contact:'), trim it off. Only
    triggers when what's left after trimming is itself a known TLD, so real
    (if unusual) domains like '.museum' or '.technology' are left alone."""
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    labels = domain.split(".")
    if labels:
        last = labels[-1]
        low = last.lower()
        for w in _GLUE_WORDS:
            if low.endswith(w) and len(low) > len(w) and low[: -len(w)] in COMMON_TLDS:
                labels[-1] = last[: -len(w)]
                break
    return local + "@" + ".".join(labels)
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
    e = e.strip().strip(".,;:()[]<>").lower()
    return strip_glued_suffix(e)


def find_emails(text):
    return [clean_email(e) for e in EMAIL_RE.findall(text or "")]


def flat_text(el):
    # IMPORTANT: join with a space BEFORE collapsing whitespace, not after.
    # el.itertext() yields one string per text node; JATS/XML very often has
    # no whitespace text node between adjacent tags (e.g. a footnote number
    # in <sup> right next to an <email> tag), so joining with "" first would
    # fuse them into one token (e.g. "1jane.doe@uni.edu"). Joining each piece
    # with a space first keeps them separate, and the final .split()/" ".join
    # still collapses any real double-spaces back down to one.
    return " ".join(" ".join(el.itertext()).split())


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
# A couple of dropdown entries are data sources, not real journal/publisher
# names - Europe PMC has no JOURNAL/PUBLISHER value for these, so they're
# mapped to the field that actually restricts by source instead.
SOURCE_FILTERS = {
    "PubMed": 'SRC:MED',
    "Europe PMC": '(SRC:PMC OR SRC:PPR)',
}


def _date_bound(d, open_end):
    """Turn what's in a date field into a bound for FIRST_PDATE, or None if
    the field is empty (meaning "no limit on this side")."""
    d = (d or "").strip()
    if not d:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
        return d
    if re.fullmatch(r"\d{4}", d):
        return d + ("-12-31" if open_end else "-01-01")
    return None


def build_query(term, journals, oa, from_date, to_date):
    if re.search(r"\b(AND|OR|NOT)\b|[\"():\[\]]", term):
        core = f"({term})"  # already advanced Europe PMC syntax - use as typed
    elif len(term.split()) == 1:
        core = term
    else:
        core = f'"{term}"'  # multiple words -> exact phrase, not "any word matches"
    q = core
    lo = _date_bound(from_date, open_end=False)
    hi = _date_bound(to_date, open_end=True)
    if lo or hi:
        # No date filter at all by default - only add one once the person
        # actually fills in a from/to date. An open side uses "*" (no bound).
        q += f" AND (FIRST_PDATE:[{lo or '*'} TO {hi or '*'}])"
    if journals:
        clauses = []
        for j in journals:
            if j in SOURCE_FILTERS:
                clauses.append(SOURCE_FILTERS[j])
            else:
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


def fetch_rows_for_record(rec):
    """Everything needed from ONE article: full-text (if open access) plus the
    affiliation-text fallback. Runs in a worker thread - no shared state
    besides plain HTTP calls, so many can run at once safely."""
    rows = []
    if rec.get("pmcid") and rec.get("isOpenAccess") == "Y":
        xml_text = http_get_text(f"{UPSTREAM}/{rec['pmcid']}/fullTextXML")
        if xml_text:
            try:
                rows += authors_from_fulltext(xml_text)
            except Exception:
                pass
    rows += authors_from_affiliations(rec)
    return rows


def run_search(job_id, user_id, terms, journals, oa, syn, from_date, to_date, max_per_term, stop_flag):
    """Core scan loop, shared by manual runs and their pause/resume."""
    total_new_articles = 0
    total_new_emails = 0
    per_term = []

    for term in terms:
        if stop_flag and stop_flag.is_set():
            break
        query = build_query(term, journals, oa, from_date, to_date)
        qkey = query + ("|syn" if syn else "")

        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id]["status"] = f'"{term}": counting matches...'
                JOBS[job_id].setdefault("qkeys", set()).add(qkey)  # so Stop can clear resume-progress

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
        note = None

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
                data = http_get_json(url, tries=4)
            except Exception as e:
                # Stop this search word here rather than crashing the whole
                # job. Deliberately do NOT reset the cursor back to the
                # start - that would silently re-scan everything already
                # done and, if Europe PMC keeps failing at this same point
                # (e.g. a deep-pagination limit on very large result sets),
                # would retry forever instead of stopping. Leaving the
                # cursor untouched means a later run can safely try again
                # from exactly this spot.
                note = (f"Stopped after {new_art:,} articles - Europe PMC stopped responding "
                        f"at this point ({e}). Already-collected results are kept; running this "
                        f"search again will pick up from here.")
                with JOBS_LOCK:
                    if job_id in JOBS:
                        JOBS[job_id]["status"] = f'"{term}": {note}'
                break
            results = data.get("resultList", {}).get("result", [])
            if not results:
                save_progress(conn, user_id, qkey, cursor, scanned, True)
                conn.commit()
                break

            # Figure out which articles in this page are actually new, then
            # fetch their full text in parallel (chunked, so Pause responds
            # within one chunk instead of waiting out a whole 100-article page).
            #
            # `page_interrupted` tracks whether we stopped partway through this
            # page (paused, or hit the max-articles cap mid-page). If so, we
            # must NOT advance the saved cursor to the next page - the cursor
            # already on record still points at THIS page, so resuming
            # re-fetches it and (thanks to is_done) only re-does the leftover
            # articles. Advancing the cursor anyway would silently and
            # permanently skip whatever was left unprocessed in this page.
            page_interrupted = False
            fresh = []
            for rec in results:
                if stop_flag and stop_flag.is_set():
                    page_interrupted = True
                    break
                if max_per_term and (new_art + len(fresh)) >= max_per_term:
                    page_interrupted = True
                    break
                if is_done(conn, user_id, rec):
                    skipped += 1
                    continue
                fresh.append(rec)

            i = 0
            while i < len(fresh) and not (stop_flag and stop_flag.is_set()):
                chunk = fresh[i:i + CHUNK_SIZE]
                if max_per_term:
                    budget = max(0, max_per_term - new_art)
                    if len(chunk) > budget:
                        chunk = chunk[:budget]
                        page_interrupted = True
                if not chunk:
                    break
                with ThreadPoolExecutor(max_workers=FULLTEXT_WORKERS) as ex:
                    future_map = {ex.submit(fetch_rows_for_record, rec): rec for rec in chunk}
                    for fut in as_completed(future_map):
                        rec = future_map[fut]
                        try:
                            rows = fut.result()
                        except Exception:
                            rows = []
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
                i += CHUNK_SIZE
            if i < len(fresh) or (stop_flag and stop_flag.is_set()):
                page_interrupted = True

            if page_interrupted:
                break  # cursor/scanned left exactly as they were - safe to resume

            nxt = data.get("nextCursorMark")
            scanned += len(results)
            if not nxt or nxt == cursor:
                save_progress(conn, user_id, qkey, nxt if nxt else cursor, scanned, True)
                conn.commit()
                break
            cursor = nxt
            save_progress(conn, user_id, qkey, cursor, scanned, False)
            conn.commit()

        conn.close()
        total_new_articles += new_art
        total_new_emails += new_em
        entry = {"term": term, "all_hits": all_hits, "new_articles": new_art,
                 "new_emails": new_em, "skipped": skipped}
        if note:
            entry["note"] = note
        per_term.append(entry)

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


def start_job(user_id, terms, journals, oa, syn, from_date, to_date, max_per_term):
    """Common helper: register a job and run it in a background thread. Returns job_id."""
    job_id = new_job_id()
    stop_flag = threading.Event()
    with JOBS_LOCK:
        JOBS[job_id] = {
            "state": "queued", "status": "Queued...", "stop_flag": stop_flag,
            "user_id": user_id,
        }
    params = dict(
        user_id=user_id, terms=terms, journals=journals, oa=oa, syn=syn,
        from_date=from_date, to_date=to_date, max_per_term=max_per_term,
    )
    t = threading.Thread(target=job_worker, args=(job_id, params), daemon=True)
    t.start()
    return job_id


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
    oa = request.args.get("oa") == "1"
    syn = request.args.get("syn") == "1"
    from_date = request.args.get("from", "")
    to_date = request.args.get("to", "")
    if not term:
        return jsonify({"error": "missing term"}), 400
    q = build_query(term, journals, oa, from_date, to_date)
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
        oa=bool(body.get("oa", True)),
        syn=bool(body.get("syn", True)),
        from_date=body.get("from", ""),
        to_date=body.get("to", ""),
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
    """Pause: interrupt the run, but keep its saved progress so a later
    Resume (just running the same search again) continues from here."""
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        if not j:
            return jsonify({"error": "unknown job"}), 404
        j["stop_flag"].set()
    return jsonify({"ok": True})


@app.route("/api/job/<job_id>/abort", methods=["POST"])
def api_job_abort(job_id):
    """Stop: interrupt the run AND forget where it was up to, so the next
    time this search is run it starts over from the beginning. Already
    collected emails/articles are NOT deleted - only the "resume point"."""
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        if not j:
            return jsonify({"error": "unknown job"}), 404
        j["stop_flag"].set()
        user_id = j.get("user_id")
        qkeys = list(j.get("qkeys", []))

    def _finish_abort():
        for _ in range(100):  # wait up to ~10s for the run loop to actually exit
            with JOBS_LOCK:
                state = JOBS.get(job_id, {}).get("state")
            if state in ("stopped", "done", "error"):
                break
            time.sleep(0.1)
        if user_id and qkeys:
            conn = get_conn()
            for qk in qkeys:
                conn.execute("DELETE FROM query_progress WHERE user_id=? AND qkey=?", (user_id, qk))
            conn.commit()
            conn.close()
        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id]["state"] = "aborted"
                JOBS[job_id]["status"] = "Stopped. Starting this search again will begin from the beginning."

    threading.Thread(target=_finish_abort, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/clear", methods=["POST"])
def api_clear():
    """Clear my history: wipes this browser's collected articles/emails and
    resume-progress. Does not touch other visitors' data."""
    conn = get_conn()
    conn.execute("DELETE FROM articles WHERE user_id=?", (g.user_id,))
    conn.execute("DELETE FROM emails WHERE user_id=?", (g.user_id,))
    conn.execute("DELETE FROM query_progress WHERE user_id=?", (g.user_id,))
    conn.commit()
    conn.close()
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


# ---------------------------------------------------------------------- page
PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Europe PMC Author Email Collector</title>
<style>
  :root {
    color-scheme: light dark;
    --bd: #8884;
    --ac: #2a6fdb;
    --ac-hover: #1d5bc0;
    --danger: #d64545;
    --tint: rgba(127,127,127,.05);
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    max-width: 980px; margin: 32px auto 64px; padding: 0 20px; line-height: 1.45;
  }
  h1 { font-size: 1.5rem; margin: 0 0 4px; letter-spacing: -.01em; }
  p.sub { margin: 0 0 20px; opacity: .6; font-size: .92rem; }
  h2 { font-size: .82rem; margin: 0 0 14px; opacity: .8; text-transform: uppercase; letter-spacing: .06em; font-weight: 700; }
  .card { border: 1px solid var(--bd); border-radius: 10px; padding: 18px 20px; margin-bottom: 18px; background: var(--tint); }
  #stats { font-size: .85rem; padding: 10px 14px; border: 1px solid var(--bd); border-radius: 8px; margin-bottom: 18px; opacity: .85; }
  label { display: block; font-weight: 600; font-size: .82rem; margin: 14px 0 5px; }
  label:first-of-type { margin-top: 0; }
  textarea, input[type=number], input[type=text], input[type=date] {
    width: 100%; padding: 9px 10px; border: 1px solid var(--bd); border-radius: 7px;
    font: inherit; font-size: .92rem; background: transparent; color: inherit;
  }
  textarea { height: 78px; resize: vertical; }
  .row { display: flex; gap: 14px; flex-wrap: wrap; }
  .row > div { flex: 1 1 150px; }
  .chk { font-weight: 400; display: flex; gap: 8px; align-items: center; margin-top: 12px; font-size: .88rem; }
  .hint { font-weight: 400; opacity: .55; font-size: .78rem; }
  .btn-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 16px; }
  button {
    padding: 9px 16px; border: 0; border-radius: 7px; font: inherit; font-size: .88rem;
    font-weight: 600; cursor: pointer; background: var(--ac); color: #fff;
  }
  button:hover:not(:disabled) { background: var(--ac-hover); }
  button.sec { background: transparent; color: inherit; border: 1px solid var(--bd); }
  button.sec:hover:not(:disabled) { background: var(--tint); }
  button.danger { background: transparent; color: var(--danger); border: 1px solid var(--danger); }
  button.danger:hover:not(:disabled) { background: rgba(214,69,69,.08); }
  button:disabled { opacity: .4; cursor: default; }
  #status { margin: 14px 0 0; font-size: .88rem; min-height: 1.3em; opacity: .85; }
  .tw { overflow-x: auto; border: 1px solid var(--bd); border-radius: 8px; max-height: 320px; overflow-y: auto; }
  table { border-collapse: collapse; width: 100%; font-size: .82rem; }
  th, td { text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--bd); white-space: nowrap; max-width: 320px; overflow: hidden; text-overflow: ellipsis; }
  th { position: sticky; top: 0; background: Canvas; font-weight: 600; opacity: .8; }
  tr:last-child td { border-bottom: none; }
  #journalBox { border: 1px solid var(--bd); border-radius: 7px; max-height: 200px; overflow-y: auto; padding: 8px 10px; }
  #journalBox label { font-weight: 400; margin: 3px 0; display: flex; gap: 7px; align-items: center; font-size: .86rem; }
  .catHead { font-weight: 700; margin-top: 10px; opacity: .7; font-size: .74rem; text-transform: uppercase; letter-spacing: .04em; }
  .catHead:first-child { margin-top: 0; }
</style>
</head>
<body>
<h1>Europe PMC Author Email Collector</h1>
<p class="sub">Your own collected data on this browser - not shared with other visitors.</p>
<div id="stats">Loading your stats...</div>

<div class="card">
<h2>Search</h2>
<label for="terms">Search words (one per line)</label>
<textarea id="terms" placeholder="e.g. nano materials&#10;bio chemistry&#10;artificial intelligence"></textarea>
<div class="hint" style="margin-top:4px;">Searches every year by default. Set a date range below only if you want to narrow it down.</div>

<label for="journalFilter">Journals / publishers (optional - pick from your list, or leave empty for all)</label>
<input type="text" id="journalFilter" placeholder="Type to filter the list below...">
<div id="journalBox"></div>

<div class="row">
  <div><label for="from">From date <span class="hint">(optional)</span></label><input type="date" id="from"></div>
  <div><label for="to">To date <span class="hint">(optional)</span></label><input type="date" id="to"></div>
  <div><label for="max">Max NEW articles per search word (0 = all)</label><input type="number" id="max" value="0" min="0"></div>
</div>

<div class="tw" style="margin-top:14px;"><table>
  <thead><tr><th>Search word</th><th>Total matches on Europe PMC</th><th>New articles this run</th><th>New emails this run</th><th>Already collected (skipped)</th></tr></thead>
  <tbody id="termsBody"></tbody>
</table></div>

<label class="chk"><input type="checkbox" id="oa" checked> Open-access articles only (full text available, far more emails)</label>
<label class="chk"><input type="checkbox" id="syn" checked> Include synonyms</label>

<div class="btn-row">
  <button id="go">Start</button>
  <button id="stopBtn" class="sec" disabled>Stop</button>
</div>
<div class="btn-row">
  <button id="dlRun" class="sec" disabled>Download (CSV)</button>
  <button id="clearBtn" class="danger">Clear my history</button>
</div>

<div id="status"></div>
</div>

<div class="card">
<h2>My most recently collected emails</h2>
<div class="tw"><table>
  <thead><tr><th>Author</th><th>Email</th><th>Article</th><th>Search word</th><th>Added</th></tr></thead>
  <tbody id="tb"></tbody>
</table></div>
</div>


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
    cb.addEventListener("change", scheduleAutoCheck);
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

function params() {
  return {
    terms: $("terms").value.split("\n").map(s => s.trim()).filter(Boolean),
    journals: selectedJournals(),
    oa: $("oa").checked,
    syn: $("syn").checked,
    from: $("from").value || "",
    to: $("to").value || "",
    max: parseInt($("max").value) || 0,
  };
}

const fmt = v => (v === null || v === undefined) ? "-" : (typeof v === "number" ? v.toLocaleString() : v);
function renderTermStats(rows) {
  const tb = $("termsBody"); tb.innerHTML = "";
  rows.forEach(r => {
    const tr = document.createElement("tr");
    const cells = r.error
      ? [r.term, "error: " + r.error, "-", "-", "-"]
      : [r.term, fmt(r.all_hits), fmt(r.new_articles), fmt(r.new_emails), fmt(r.skipped)];
    cells.forEach(v => { const td = document.createElement("td"); td.textContent = v; tr.appendChild(td); });
    if (r.note) { const td = document.createElement("td"); td.textContent = r.note; td.style.whiteSpace = "normal"; tr.appendChild(td); }
    tb.appendChild(tr);
  });
}
function hitcountURL(term, p) {
  const q = new URLSearchParams();
  q.set("term", term); q.set("oa", p.oa ? "1" : "0");
  q.set("syn", p.syn ? "1" : "0"); q.set("from", p.from); q.set("to", p.to);
  p.journals.forEach(j => q.append("journal", j));
  return "/api/hitcount?" + q.toString();
}

// Counts update automatically as you type/change filters - no button needed.
let autoCheckTimer = null, autoCheckSeq = 0;
function scheduleAutoCheck() {
  clearTimeout(autoCheckTimer);
  autoCheckTimer = setTimeout(autoCheckMatches, 500);
}
async function autoCheckMatches() {
  const mySeq = ++autoCheckSeq;
  const p = params();
  if (!p.terms.length) { renderTermStats([]); return; }
  const rows = [];
  for (const term of p.terms) {
    if (mySeq !== autoCheckSeq) return; // a newer keystroke superseded this check
    try {
      const d = await (await fetch(hitcountURL(term, p))).json();
      if (mySeq !== autoCheckSeq) return;
      rows.push(d.error ? { term, error: d.error } : { term, all_hits: d.hitCount, new_articles: null, new_emails: null, skipped: null });
    } catch (e) {
      if (mySeq !== autoCheckSeq) return;
      rows.push({ term, error: e.message });
    }
    renderTermStats(rows);
  }
}
$("terms").addEventListener("input", scheduleAutoCheck);
["oa", "syn", "from", "to"].forEach(id => $(id).addEventListener("change", scheduleAutoCheck));

// Single button: Start -> Pause (while running) -> Resume (after paused) -> Pause -> ...
// "Pause" just stops the current job early; since progress is saved as it
// goes, clicking "Resume" (which re-runs the same search) picks up exactly
// where it left off instead of starting over. The separate "Stop" button
// aborts instead: it also halts the job, but forgets where it was up to, so
// the next Start begins that search from scratch. Either way, anything
// already collected before stopping/pausing stays collected.
let runState = "idle"; // idle | running | paused
function setRunButton() {
  const b = $("go");
  if (runState === "running") { b.textContent = "Pause"; b.disabled = false; }
  else if (runState === "paused") { b.textContent = "Resume"; b.disabled = false; }
  else { b.textContent = "Start"; b.disabled = false; }
  $("stopBtn").disabled = (runState === "idle");
}

function beginPolling(job_id) {
  currentJob = job_id;
  runState = "running"; setRunButton();
  $("dlRun").disabled = true;
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
async function pause() {
  if (currentJob) await fetch("/api/job/" + currentJob + "/stop", { method: "POST" });
}
async function goButtonClick() {
  if (runState === "running") await pause();
  else await start(); // covers both "Start" (idle) and "Resume" (paused) - same action, same query resumes from saved progress
}
async function abortJob() {
  if (!currentJob) return;
  $("status").textContent = "Stopping...";
  await fetch("/api/job/" + currentJob + "/abort", { method: "POST" });
}
async function checkJob() {
  if (!currentJob) return;
  const d = await (await fetch("/api/job/" + currentJob)).json();
  $("status").textContent = d.status || "";
  if (d.result && d.result.per_term) renderTermStats(d.result.per_term);
  if (d.state === "done" || d.state === "stopped" || d.state === "error" || d.state === "aborted") {
    clearInterval(poll);
    runState = (d.state === "stopped") ? "paused" : "idle";
    setRunButton();
    if (d.state === "done" || d.state === "aborted") $("dlRun").disabled = false;
    loadStats(); loadRecent();
  }
}
async function clearHistory() {
  if (!confirm("Clear everything you've collected on this browser (articles, emails, and search progress)? This can't be undone.")) return;
  await fetch("/api/clear", { method: "POST" });
  $("termsBody").innerHTML = ""; $("status").textContent = "History cleared.";
  $("dlRun").disabled = true; currentJob = null; runState = "idle"; setRunButton();
  loadStats(); loadRecent();
}

$("go").onclick = goButtonClick;
$("stopBtn").onclick = abortJob;
$("dlRun").onclick = () => { if (currentJob) location.href = "/api/download/job/" + currentJob + ".csv"; };
$("clearBtn").onclick = clearHistory;

setRunButton();
loadJournals(); loadStats(); loadRecent();
setInterval(loadStats, 30000);
</script>
</body>
</html>
"""

init_db()

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