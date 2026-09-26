# Europe PMC Author Email Collector - deployment guide

## What you have
- `app.py` - the whole application (web page + backend, one file)
- `journals.json` - your journal/publisher list, built from your spreadsheet
  (269 unique entries, across "General", "Engineering" and "3D Printing")
- `requirements.txt` - the two packages it needs
- `collector.db` - created automatically the first time it runs (SQLite).
  This is where every collected article and email lives. Back this file up.

All three files must stay in the same folder.

## 1. Run it locally first
```
pip install -r requirements.txt
python app.py
```
You'll see:
```
Starting production server (waitress) on http://0.0.0.0:8000
(No 'development server' warning - this is a real production server.)
```
That warning you saw before ("This is a development server...") was Flask's
own built-in server, meant only for coding/testing. `app.py` now starts
**waitress** (a real production WSGI server) automatically, since it's
already in `requirements.txt` - so you won't see that warning anymore. If for
some reason `waitress` isn't installed, it still falls back to the Flask dev
server and tells you so, but for real use always run with waitress installed.

Open http://127.0.0.1:8000 in a browser. Confirm the journal dropdown loads,
run a small search (a few articles), and check both download buttons work.

## 2. Run it in production (any of these)

### Option A - a small VPS (DigitalOcean, Hetzner, a spare machine, etc.)
This is the most control for the least cost (a $5-6/month box is plenty).

```
sudo apt update && sudo apt install -y python3-venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Production server (waitress), not the "python app.py" dev server:
waitress-serve --host=0.0.0.0 --port=8000 app:app
```
Keep it running after you log out, either with `systemd` (recommended) or `tmux`/`screen`.

Example systemd service (`/etc/systemd/system/epmc-collector.service`):
```ini
[Unit]
Description=Europe PMC Author Email Collector
After=network.target

[Service]
WorkingDirectory=/opt/epmc-collector
ExecStart=/opt/epmc-collector/venv/bin/waitress-serve --host=127.0.0.1 --port=8000 app:app
Restart=always
Environment=EPMC_REFRESH_SECONDS=21600

[Install]
WantedBy=multi-user.target
```
Then put Nginx (or Caddy) in front of it for HTTPS and a real domain name -
this is the standard "reverse proxy" setup and any hosting tutorial for
"deploy Flask app with Nginx" covers it.

### Option B - a platform that deploys from a folder (Render, Railway, Fly.io, PythonAnywhere)
These are simpler: you upload/connect the folder, and they run:
```
waitress-serve --host=0.0.0.0 --port=$PORT app:app
```
Two things to check on these platforms:
- **Persistent disk.** By default, many of these platforms wipe local files
  (including `collector.db`) on every redeploy. Turn on their "persistent
  disk" / "volume" feature and point `EPMC_DB_PATH` at a file inside it, e.g.
  `EPMC_DB_PATH=/data/collector.db`. Without this, you'll lose your
  collected history whenever you update the app.
- **One instance only.** Don't scale this app to multiple instances/replicas
  - SQLite is a single file and two servers writing to it at once will
    corrupt it. One instance easily handles many people browsing/searching
    at once (see "concurrency" below); if you outgrow that, see the note
    on Postgres further down.

## 3. Multiple users at once, each with their OWN history
This app is built for that, with one important design choice: **each visitor
gets their own separate collected history, not a shared pool.**

How it works: the first time someone opens the site, the server sets a
private cookie in their browser (no login, no password - just an anonymous
ID). Every article and email they collect is tagged with that ID. So:
- If two people both search "nano", they each independently collect their
  own copy of the matching articles/emails - one person's results don't get
  marked as "already collected" for the other person.
- Each person's "download all my collected data" button only ever downloads
  their own data.
- If someone clears their cookies or switches browsers/devices, they start a
  fresh, empty history - there's no account system tying it back to them.
  If you later want one login to follow someone across devices, that needs
  real user accounts (sign-in), which is a separate step from what's built
  here.

Under the hood:
- Searches run as background jobs, so one person's search doesn't block
  another's page from loading.
- By default at most 2 searches run at the same time (`EPMC_MAX_JOBS=2`), so
  the app doesn't hammer Europe PMC's servers if several people click "Start"
  together. Extra searches queue and start as soon as a slot frees up. Raise
  this with `EPMC_MAX_JOBS=4` etc. if needed - Europe PMC does have its own
  rate limits, so don't set this very high.
- SQLite in "WAL mode" (already configured) comfortably handles many readers
  and a couple of concurrent writers. For a few dozen people browsing/searching
  together this is plenty. If this ever needs to serve hundreds of people
  hammering it at once, the database can be swapped for PostgreSQL - the code
  changes are small (the SQL is plain and portable) but that's a step to take
  only if you actually hit that scale.
- One trade-off of per-user history: if ten people all search "nano", each of
  them independently re-fetches and re-processes the same articles (since
  nothing is shared between them). That's the direct cost of keeping histories
  separate - it uses more of Europe PMC's bandwidth than a shared pool would,
  but it's what was asked for.

## 4. Downloading your results
Two separate buttons, doing two different things:
- **"Download this run"** - only the new emails found by the search you just
  ran (or the watch you just created). Empty if that run found nothing new
  (e.g. you searched the same thing twice in a row).
- **"Download all my collected data"** - everything you've ever collected on
  this browser, across every run and every watch, from the beginning.

## 5. Automatic updates for new articles
Click **"Auto-refresh this search"** after setting up your search words /
journals. Two things happen:
1. **Right away**, it collects every article that currently matches your
   search - you don't wait for anything to "become new" first, you get the
   full existing backlog immediately.
2. **After that**, a background thread re-checks that same watched search on
   a timer (default every 6 hours, change with `EPMC_REFRESH_SECONDS`) and
   pulls in only whatever's been newly published since the last check - so
   new emails keep appearing automatically without anyone re-running the
   search by hand.

This background thread runs as long as the app process is running, so keep
it running continuously (systemd's `Restart=always` handles restarts after
crashes or reboots).

## Environment variables (all optional)
| Variable | Default | Meaning |
|---|---|---|
| `PORT` | 8000 | Port the dev server listens on (`python app.py` only) |
| `EPMC_DB_PATH` | `collector.db` next to app.py | Where the shared database file lives |
| `EPMC_REFRESH_SECONDS` | 21600 (6 hours) | How often watched searches are re-checked |
| `EPMC_MAX_JOBS` | 2 | Max searches running at the same time |
| `EPMC_RUN_SCHEDULER` | 1 | Set to `0` to disable the auto-refresh thread entirely |

## Notes and honest limitations
- The journal dropdown filters by Europe PMC's `JOURNAL:` and `PUBLISHER:`
  fields. Real journal titles (e.g. "Natural and Engineering Sciences") match
  well. A few entries in your spreadsheet are publisher *homepages* rather
  than journal titles (e.g. "Elsevier", "Springer") - Europe PMC's
  `PUBLISHER:` field catches most of these, but a small number may return
  fewer results than you'd expect. If a particular journal in the dropdown
  returns zero, that's the likely reason.
- Emails still only come from what authors publish (mainly corresponding
  authors, mainly in open-access articles) - nothing about this rewrite
  changes what data exists, only how reliably it's collected.
- User identity is a plain browser cookie, not a secure login. It's enough to
  keep one visitor's results separate from another's on normal use, but
  anyone who deliberately copies someone else's cookie value could see their
  data. Fine for an internal collection tool; not something to expose
  publicly if the collected emails are sensitive.
- This has been tested against a simulated Europe PMC (since this
  environment can't reach the real one), covering: the query builder, email
  extraction, per-user isolation (two independent "browsers" each getting
  their own history from the same search), the two separate downloads, and
  a watch immediately collecting the full existing backlog on creation. It
  has **not** been tested against the real, live Europe PMC service - test
  with a small search (a handful of articles) before relying on it for a
  big run.
- If you ran the earlier version of this app and already have a
  `collector.db` file from testing, delete it before your real deployment -
  its table structure doesn't have the per-user columns this version needs.
