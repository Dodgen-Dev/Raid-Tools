"""
Comprehensive Raid Data Pull — Team Detention
Auto-discovers reports within the configured date range (start_date → end_date).
Pulls from WCL + Blizzard API.
Stores everything in pandas DataFrames (raid_dataframes.xlsx with _dtypes schema).
Updates roster.json with new characters.

Resume support: saves checkpoint .pkl after each expensive phase.
If interrupted, re-run and it picks up where it left off.
Delete raid_pull_resume.pkl to force a fresh run.

Usage: python raid_pull.py
       python raid_pull.py --trial    (first report only — for troubleshooting)
Requires: config.json with warcraftlogs + blizzard credentials
Output:  raid_dataframes.xlsx (pandas DataFrames + _dtypes schema), roster.json (updated)
"""
import json, os, re, requests, time, pickle, sys
from datetime import datetime, timezone
from collections import defaultdict

# Force UTF-8 output regardless of Windows console encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def load_raid_dataframes(path="raid_dataframes.xlsx"):
    """Load raid DataFrames from xlsx with type restoration via _dtypes sheet.

    Usage:
        from raid_pull import load_raid_dataframes
        dfs = load_raid_dataframes()
        df_fights = dfs["fights"]
        df_player_dt = dfs["player_damage_taken"]
    Or standalone:
        import pandas as pd
        # (copy this function into your script)
    """
    import pandas as pd
    raw = pd.read_excel(path, sheet_name=None)
    schema = raw.pop("_dtypes", None)
    if schema is None:
        return raw  # no schema sheet — return as-is
    for name, df in raw.items():
        sheet_schema = schema[schema["sheet"] == name]
        for _, row in sheet_schema.iterrows():
            col, dtype = row["column"], row["dtype"]
            if col not in df.columns:
                continue
            try:
                if dtype == "bool":
                    df[col] = df[col].astype(bool)
                elif dtype == "int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "object":
                    df[col] = df[col].where(df[col].notna(), None)
            except Exception:
                pass  # leave as-is if restoration fails
        raw[name] = df
    return raw


def _read_json(path):
    """Read a JSON file, handling both UTF-8 and Windows cp1252 encoding."""
    raw = open(path, "rb").read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")
        try:
            with open(path, "w", encoding="utf-8") as fix:
                fix.write(text)
        except OSError:
            pass
    return json.loads(text)


# ── Trial mode ──
TRIAL_MODE = "--trial" in sys.argv
if TRIAL_MODE:
    print("⚡ TRIAL MODE — limiting to first report only")

# ══════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════
config = _read_json("config.json")

# ── Guild IDs (from config.json) ──
TEAM_GUILD_ID = config.get("team_guild_id", None)
PARENT_GUILD_ID = config.get("parent_guild_id", None)
GUILD_TAG_ID = config.get("guild_tag_id", None)

if not TEAM_GUILD_ID and not PARENT_GUILD_ID:
    print("ERROR: No team_guild_id or parent_guild_id in config.json")
    print("  Set these in the Config tab → WCL Guild IDs section")
    sys.exit(1)

INCLUDE_PARENT_GUILD = config.get("include_parent_guild", False)

# Primary ID to try first, fallback second
GUILD_ID = TEAM_GUILD_ID or PARENT_GUILD_ID
FALLBACK_GUILD_ID = PARENT_GUILD_ID if TEAM_GUILD_ID else None

# ── Guild info from roster.json (for guildData query + meta) ──
ROSTER_FILE = "roster.json"
_roster_team = {}
_roster_data = {}
if os.path.isfile(ROSTER_FILE):
    _roster_data = _read_json(ROSTER_FILE)
    _roster_team = _roster_data.get("team", {})
# Config.json is primary source; roster.json is fallback
GUILD_NAME = config.get("guild_name", "") or _roster_team.get("guild_name", "")
TEAM_NAME = config.get("team_name", "") or _roster_team.get("team_name", "")
_server_raw = config.get("server", "") or _roster_team.get("server", "")
SERVER_SLUG = _server_raw.lower().replace("'", "").replace(" ", "-")
SERVER_REGION = (config.get("region", "") or _roster_team.get("region", "US")).upper()
ZONE_ID = 44               # Manaforge Omega

# ── DATE FILTER (from config.json) ──
# Reads start_date / end_date from config.  Falls back to patch_date for start.
# Format: YYYY-MM-DD.  Converted to UNIX ms for WCL startTime/endTime filters.
_start_str = config.get("start_date", "").strip() or config.get("patch_date", "").strip()
_end_str = config.get("end_date", "").strip()

if _start_str:
    try:
        PARTITION_START_MS = int(datetime.strptime(_start_str, "%Y-%m-%d")
                                 .replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        print(f"WARNING: Invalid start_date '{_start_str}' — expected YYYY-MM-DD")
        sys.exit(1)
else:
    print("ERROR: No start_date or patch_date in config.json")
    sys.exit(1)

if _end_str:
    try:
        # End of that day (23:59:59.999)
        PARTITION_END_MS = int((datetime.strptime(_end_str, "%Y-%m-%d")
                                .replace(tzinfo=timezone.utc).timestamp()
                                + 86400) * 1000 - 1)
    except ValueError:
        print(f"WARNING: Invalid end_date '{_end_str}' — expected YYYY-MM-DD")
        sys.exit(1)
else:
    PARTITION_END_MS = None  # no end filter — pull to present

# ── File paths ──
RESUME_PKL = "raid_pull_resume.pkl"

# ── Consumable spell IDs ──
# Read from config.json if available, otherwise use defaults.
_con_cfg = config.get("consumables", {})
CONSUMABLE_IDS = {
    "tempered_potion": _con_cfg.get("tempered_potion",
                                     [431932, 431914, 431934, 431936]),
    "healing_potion": _con_cfg.get("healing_potion", [431416, 431418]),
    "healthstone": _con_cfg.get("healthstone", [6262]),
}
# Flat list for query filter
ALL_CONSUMABLE_IDS = []
for ids in CONSUMABLE_IDS.values():
    ALL_CONSUMABLE_IDS.extend(ids)
CONSUMABLE_FILTER_EXPR = "ability.id in (%s)" % ",".join(str(i) for i in ALL_CONSUMABLE_IDS)
# Reverse lookup: spell_id → category (for tagging rows)
_CONSUMABLE_CATEGORY = {}
for cat, ids in CONSUMABLE_IDS.items():
    for sid in ids:
        _CONSUMABLE_CATEGORY[sid] = cat

# ── Mechanic-specific query config ──
# Only these encounters get extra queries in Phase 4.1.
# Interrupts: encounter IDs that need per-fight interrupt data
MECHANIC_INTERRUPT_ENCOUNTERS = {
    3134,  # Nexus-King Salhadaar
}
# Target swap: encounter ID → list of mob names to track DamageDone against
# Uses filterExpression at API level so response is small.
MECHANIC_TARGET_SWAP = {
    3129: ["Volatile Manifestation"],   # Plexus Sentinel (GUID 243241)
    3131: ["Infused Tangle"],           # Loom'ithar (GUID 245173)
    3135: ["Living Mass"],              # Dimensius, the All-Devouring
}

# Tank swap debuffs: encounter ID → list of debuff ability names to track
# Used to build the "debuffs" DataFrame for tank swap scoring in build_tracker.
# Debuff names must match WCL ability names exactly.
TANK_SWAP_DEBUFFS = {
    3129: ["Obliteration Arcanocannon"],            # Plexus Sentinel
    3131: ["Piercing Strand", "Writhing Wave"],     # Loom'ithar
    3130: ["Mystic Lash"],                          # Soulbinder Naazindhri
    3132: ["Overwhelming Power"],                   # Forgeweaver Araz
    3122: ["Fel-Singed", "Shattered Soul"],          # The Soul Hunters
    3133: ["Shockwave Slam"],                        # Fractillus
    3134: ["Conquer", "Vanquish"],                   # Nexus-King Salhadaar
    3135: ["Mortal Fragility", "Touch of Oblivion"], # Dimensius, the All-Devouring
}

# ── Known roster (built from roster.json — mains + alts + unlinked) ──
KNOWN_ROSTER = set()
for _pdata in _roster_data.get("players", {}).values():
    for _c in _pdata.get("mains", []):
        KNOWN_ROSTER.add(_c.lower())
    for _c in _pdata.get("alts", []):
        KNOWN_ROSTER.add(_c.lower())
for _c in _roster_data.get("unlinked", []):
    KNOWN_ROSTER.add(_c.lower())

ROSTER_LOCKED = _roster_data.get("meta", {}).get("locked", False)

# ── Roster threshold: skip fights with fewer than N roster players ──
MIN_ROSTER_PLAYERS = int(config.get("min_roster_players", 0))

# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════
def esc(s):
    return s.replace('\\', '\\\\').replace('"', '\\"')

def to_realm_slug(wcl_server):
    s = wcl_server.replace("'", "")
    s = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '-', s)
    slug = s.lower()
    if slug == "area52":
        slug = "area-52"
    return slug

def ts():
    return datetime.now(tz=timezone.utc).strftime('%H:%M:%S')

def ts_full():
    return datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def elapsed(start):
    secs = time.time() - start
    if secs < 60: return f"{secs:.0f}s"
    return f"{secs/60:.1f}m"

# ── Counters ──
wcl_query_count = 0
api_start_time = time.time()

# ══════════════════════════════════════════════════════════════════════
#  RESUME / CHECKPOINT
# ══════════════════════════════════════════════════════════════════════
_PHASE1_VARS = ["REPORT_CODES", "wcl_query_count"]
_PHASE2_VARS = _PHASE1_VARS + ["reports_data"]
_PHASE3_VARS = _PHASE2_VARS
_PHASE4_VARS = _PHASE3_VARS
_PHASE45_VARS = _PHASE4_VARS
_PHASE5_VARS = _PHASE45_VARS + ["player_info"]
_PHASE7_VARS = _PHASE5_VARS
_PHASE8_VARS = _PHASE7_VARS + ["guild_rankings"]


def _pickle_safe(val):
    if isinstance(val, defaultdict):
        return {k: _pickle_safe(v) for k, v in val.items()}
    if isinstance(val, dict):
        return {k: _pickle_safe(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_pickle_safe(v) for v in val]
    return val


def save_resume(next_phase, var_names):
    state = {
        "_resume_from": next_phase,
        "_saved_at": ts_full(),
    }
    for name in var_names:
        if name in globals():
            state[name] = _pickle_safe(globals()[name])
    with open(RESUME_PKL, "wb") as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
    sz = os.path.getsize(RESUME_PKL) / (1024 * 1024)
    print(f"[{ts()}]   💾 Checkpoint → {RESUME_PKL} ({sz:.1f} MB) — resume at phase {next_phase}")


def load_resume():
    with open(RESUME_PKL, "rb") as f:
        state = pickle.load(f)
    resume_from = state.pop("_resume_from")
    saved_at = state.pop("_saved_at", "unknown")
    globals().update(state)
    return resume_from, saved_at


# ══════════════════════════════════════════════════════════════════════
#  API CLIENTS
# ══════════════════════════════════════════════════════════════════════
print(f"[{ts()}] Authenticating with WCL...")
wcl_token = None
for attempt in range(3):
    try:
        r = requests.post("https://www.warcraftlogs.com/oauth/token",
                          data={"grant_type": "client_credentials"},
                          auth=(config["warcraftlogs"]["client_id"],
                                config["warcraftlogs"]["client_secret"]),
                          timeout=15)
        wcl_token = r.json()["access_token"]
        print(f"[{ts()}]   ✓ WCL token acquired")
        break
    except Exception as e:
        print(f"[{ts()}]   ✗ WCL auth attempt {attempt+1} failed: {e}")
        time.sleep(2)
if not wcl_token:
    print(f"[{ts()}] FATAL: Could not authenticate with WCL"); sys.exit(1)


def wcl_q(query):
    global wcl_query_count
    for attempt in range(3):
        try:
            r = requests.post("https://www.warcraftlogs.com/api/v2/client",
                              json={"query": query},
                              headers={"Authorization": f"Bearer {wcl_token}"},
                              timeout=30)
            wcl_query_count += 1
            d = r.json()
            if "errors" in d:
                print(f"[{ts()}]   ⚠ WCL GQL error: {json.dumps(d['errors'])[:200]}")
            return d
        except requests.exceptions.Timeout:
            print(f"[{ts()}]   WCL timeout (attempt {attempt+1}/3), retrying...")
            time.sleep(3)
        except Exception as e:
            print(f"[{ts()}]   WCL error (attempt {attempt+1}/3): {e}")
            time.sleep(2)
    print(f"[{ts()}]   ✗ WCL query failed after 3 attempts")
    return {}


print(f"[{ts()}] Authenticating with Blizzard...")
bliz_token = None
for attempt in range(3):
    try:
        r = requests.post("https://oauth.battle.net/token",
                          data={"grant_type": "client_credentials"},
                          auth=(config["blizzard"]["client_id"],
                                config["blizzard"]["client_secret"]),
                          timeout=15)
        bliz_token = r.json()["access_token"]
        print(f"[{ts()}]   ✓ Blizzard token acquired")
        break
    except Exception as e:
        print(f"[{ts()}]   ✗ Blizzard auth attempt {attempt+1} failed: {e}")
        time.sleep(2)
if not bliz_token:
    print(f"[{ts()}]   WARNING: Blizzard auth failed — gear data will be skipped")


def bliz_get(url, params=None):
    p = params or {}
    p["namespace"] = "profile-us"
    p["locale"] = "en_US"
    hdrs = {"Authorization": f"Bearer {bliz_token}"}
    for attempt in range(3):
        try:
            r = requests.get(url, params=p, headers=hdrs, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                print(f"[{ts()}]   Blizzard rate-limited, sleeping 2s...")
                time.sleep(2); continue
            else:
                return {"error": r.status_code, "reason": r.text[:200]}
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < 2: time.sleep(1); continue
    return {"error": "timeout"}



# ══════════════════════════════════════════════════════════════════════
#  RESUME DETECTION
# ══════════════════════════════════════════════════════════════════════
resume_from = 1
is_resumed = False

REPORT_CODES = []
reports_data = {}
player_info = {}
guild_rankings = {}

if os.path.exists(RESUME_PKL):
    resume_from, saved_at = load_resume()
    is_resumed = True
    print(f"\n[{ts()}] 🔄 RESUMING from phase {resume_from} (saved {saved_at})")
    print(f"  WCL queries from previous run: {wcl_query_count}")
    print(f"  Reports discovered: {len(REPORT_CODES)}")
    print(f"  Reports pulled: {len(reports_data)}")
    print(f"  Players found: {len(player_info)}")
else:
    print(f"\n[{ts()}] No resume file — starting fresh run")

partition_date = datetime.fromtimestamp(PARTITION_START_MS / 1000, tz=timezone.utc)
_end_display = (datetime.fromtimestamp(PARTITION_END_MS / 1000, tz=timezone.utc)
                .strftime('%Y-%m-%d') if PARTITION_END_MS else "now")
print(f"[{ts()}] Date filter: {partition_date.strftime('%Y-%m-%d')} → {_end_display}")
_id_display = (f"Guild ID: {GUILD_ID}"
               + (f" (fallback: {FALLBACK_GUILD_ID})" if FALLBACK_GUILD_ID else "")
               + (" [+parent]" if INCLUDE_PARENT_GUILD else ""))
_name_display = " — ".join(filter(None, [GUILD_NAME, TEAM_NAME])) or "?"
print(f"[{ts()}] {_name_display} | {_id_display} | Zone: {ZONE_ID}")


# ══════════════════════════════════════════════════════════════════════
#  PHASE 1: Discover reports (with team → guild fallback)
# ══════════════════════════════════════════════════════════════════════

def _discover_reports(guild_id, label=""):
    """Query WCL for report codes under a given guildID. Returns list."""
    tag = f" ({label})" if label else ""
    print(f"[{ts()}]   Querying guildID:{guild_id}{tag}")
    codes = []
    page = 1
    while True:
        print(f"[{ts()}]   Fetching page {page}...", end=" ", flush=True)
        _end_arg = f",endTime:{PARTITION_END_MS:.0f}" if PARTITION_END_MS else ""
        d = wcl_q('''
        {reportData{reports(guildID:%d,zoneID:%d,startTime:%.0f%s,limit:100,page:%d){
          data{code title startTime}
          has_more_pages current_page last_page total
        }}}''' % (guild_id, ZONE_ID, PARTITION_START_MS, _end_arg, page))

        pagination = ((d.get("data") or {}).get("reportData") or {}).get("reports") or {}
        reports_list = pagination.get("data") or []
        total = pagination.get("total", "?")
        has_more = pagination.get("has_more_pages", False)
        last_page = pagination.get("last_page", 1)

        for r in reports_list:
            code = r.get("code", "")
            if code and code not in codes:
                codes.append(code)
                t = r.get("startTime", 0)
                dt_s = datetime.fromtimestamp(t/1000, tz=timezone.utc).strftime('%Y-%m-%d') if t else "?"
                print(f"\n[{ts()}]     {len(codes):2d}. {code}  {r.get('title','?'):40s}  {dt_s}", end="", flush=True)

        print(f"\n[{ts()}]   Page {page}/{last_page} — {len(reports_list)} reports (total: {total})")
        if not has_more:
            break
        page += 1
    return codes


if resume_from <= 1:
    phase_start = time.time()
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 1: Discovering reports (date range)")
    print(f"{'='*70}")
    print(f"[{ts()}]   startTime: {PARTITION_START_MS} ({partition_date.strftime('%Y-%m-%d')})")
    if PARTITION_END_MS:
        print(f"[{ts()}]   endTime:   {PARTITION_END_MS} ({_end_display})")

    REPORT_CODES = _discover_reports(GUILD_ID, "team")

    # ── Include parent guild logs (merge + dedup) ──
    if INCLUDE_PARENT_GUILD and FALLBACK_GUILD_ID and FALLBACK_GUILD_ID != GUILD_ID:
        print(f"\n[{ts()}]   Include parent guild enabled — also querying guildID:{FALLBACK_GUILD_ID}")
        parent_codes = _discover_reports(FALLBACK_GUILD_ID, "parent guild")
        existing = set(REPORT_CODES)
        added = [c for c in parent_codes if c not in existing]
        if added:
            REPORT_CODES.extend(added)
            print(f"[{ts()}]   Merged {len(added)} additional reports from parent guild "
                  f"(total: {len(REPORT_CODES)})")
        else:
            print(f"[{ts()}]   No additional reports from parent guild")

    # ── Fallback to parent guild if team returned nothing ──
    elif not REPORT_CODES and FALLBACK_GUILD_ID and FALLBACK_GUILD_ID != GUILD_ID:
        print(f"\n[{ts()}]   No reports under team ID {GUILD_ID} — "
              f"falling back to parent guild ID {FALLBACK_GUILD_ID}")
        REPORT_CODES = _discover_reports(FALLBACK_GUILD_ID, "parent guild")
        if REPORT_CODES:
            GUILD_ID = FALLBACK_GUILD_ID  # use parent for remainder of run
            print(f"[{ts()}]   Using parent guild ID for this run")

    print(f"\n[{ts()}] Phase 1 complete: {len(REPORT_CODES)} reports ({elapsed(phase_start)})")
    if not REPORT_CODES:
        print(f"[{ts()}] ERROR: No reports found for guildID:{GUILD_ID} zoneID:{ZONE_ID}")
        print(f"  Date range: {partition_date.strftime('%Y-%m-%d')} → {_end_display}")
        print(f"  Check that start_date in config.json isn't in the future")
        sys.exit(1)
    if TRIAL_MODE and len(REPORT_CODES) > 1:
        print(f"[{ts()}]   ⚡ TRIAL: trimming {len(REPORT_CODES)} reports → 1 ({REPORT_CODES[0]})")
        REPORT_CODES = REPORT_CODES[:1]
    save_resume(2, _PHASE1_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 2: Report metadata — fights + actors
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 2:
    phase_start = time.time()
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 2: Report metadata ({len(REPORT_CODES)} reports)")
    print(f"{'='*70}")

    already_pulled = set(reports_data.keys()) if is_resumed else set()

    for idx, code in enumerate(REPORT_CODES):
        if code in already_pulled:
            rpt = reports_data[code]
            print(f"[{ts()}]   [{idx+1}/{len(REPORT_CODES)}] {code} ({rpt['date']}) — cached, skipping")
            continue

        print(f"[{ts()}]   [{idx+1}/{len(REPORT_CODES)}] Pulling {code}...", end=" ", flush=True)
        d = wcl_q('''
        {reportData{r:report(code:"%s"){
          code title startTime endTime visibility
          guild{id name server{slug region{name}}}
          zone{id name}
          fights{
            id name encounterID difficulty kill
            startTime endTime size
            friendlyPlayers
            bossPercentage fightPercentage
            averageItemLevel
          }
          masterData{actors(type:"Player"){id name subType icon server}}
        }}}''' % esc(code))

        rpt = ((d.get("data") or {}).get("reportData") or {}).get("r")
        if not rpt:
            print(f"FAILED — no data")
            continue

        actors = {}
        for a in ((rpt.get("masterData") or {}).get("actors") or []):
            icon = a.get("icon") or ""
            parts = icon.split("-", 1) if icon else []
            actors[a["id"]] = {
                "id": a["id"], "name": a["name"],
                "class": a.get("subType") or (parts[0] if parts else ""),
                "spec": parts[1] if len(parts) > 1 else "",
                "server": a.get("server") or "",
            }

        fights = rpt.get("fights") or []
        mythic_enc = [f for f in fights
                      if f.get("difficulty") == 5 and (f.get("encounterID") or 0) > 0]
        dt = datetime.fromtimestamp(rpt["startTime"] / 1000, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")
        kills = sum(1 for f in mythic_enc if f.get("kill"))
        wipes = len(mythic_enc) - kills

        reports_data[code] = {
            "code": code, "title": rpt.get("title", ""),
            "start_time": rpt["startTime"], "end_time": rpt["endTime"],
            "date": date_str, "guild": rpt.get("guild"), "zone": rpt.get("zone"),
            "actors": actors, "fights": fights,
            "mythic_fight_ids": [f["id"] for f in mythic_enc],
            "rankings": {}, "deaths": {}, "damage_taken": {},
            "damage_taken_abilities": {},  # viewBy:Ability — boss ability catalog (full entries w/ sub-breakdowns)
            "consumable_casts": {},        # per-fight consumable usage
            "wipe_dps": {}, "wipe_hps": {},
            "mechanic_interrupts": {},     # Phase 4.1: fid → interrupt entries (specific encounters only)
            "mechanic_target_damage": {},  # Phase 4.1: fid → {mob_name: [player damage entries]}
            "tank_swap_debuffs": {},       # Phase 4.1: fid → [{debuff_name, player, applications}, ...]
        }
        print(f"✓ {date_str} | {len(fights)} fights ({len(mythic_enc)} mythic: {kills}K/{wipes}W) | {len(actors)} actors")

    total_mythic = sum(len(r["mythic_fight_ids"]) for r in reports_data.values())
    print(f"\n[{ts()}] Phase 2 complete: {len(reports_data)} reports, {total_mythic} mythic fights ({elapsed(phase_start)})")

    # ── Deduplicate fights across same-date reports ──
    # When two people log the same raid, fights appear in both reports
    # with near-identical absolute startTimes. Keep first seen, prune dupes.
    from collections import defaultdict as _dd
    reports_by_date = _dd(list)
    for code, rpt in reports_data.items():
        reports_by_date[rpt["date"]].append(code)

    dedup_total = 0
    for date, codes in reports_by_date.items():
        if len(codes) < 2:
            continue
        # Build (absolute_start_time_rounded, encounterID) → (code, fid) for first report
        seen = {}
        for code in codes:
            rpt = reports_data[code]
            rpt_start = rpt["start_time"]
            dupes_this_report = []
            for f in rpt["fights"]:
                if f.get("difficulty") != 5 or (f.get("encounterID") or 0) == 0:
                    continue
                abs_start = rpt_start + f["startTime"]
                key = (f["encounterID"], round(abs_start / 10000))  # 10s window
                if key in seen:
                    dupes_this_report.append(f["id"])
                else:
                    seen[key] = (code, f["id"])
            # Remove dupes from this report's mythic_fight_ids
            if dupes_this_report:
                before = len(rpt["mythic_fight_ids"])
                rpt["mythic_fight_ids"] = [fid for fid in rpt["mythic_fight_ids"]
                                           if fid not in dupes_this_report]
                removed = before - len(rpt["mythic_fight_ids"])
                dedup_total += removed
                print(f"[{ts()}]   Dedup {date} {code}: removed {removed} duplicate fights")

    if dedup_total:
        total_after = sum(len(r["mythic_fight_ids"]) for r in reports_data.values())
        print(f"[{ts()}]   Dedup complete: {total_mythic} → {total_after} mythic fights ({dedup_total} removed)")
    else:
        print(f"[{ts()}]   No duplicate fights found")

    # ── Roster threshold filter ──
    # Skip fights that don't have enough rostered players (multi-team guilds).
    # Only active when roster is LOCKED — unlocked means still discovering players.
    if MIN_ROSTER_PLAYERS > 0 and KNOWN_ROSTER and ROSTER_LOCKED:
        roster_removed = 0
        for code, rpt in reports_data.items():
            actors = rpt.get("actors", {})
            below_threshold = []
            for fid in rpt["mythic_fight_ids"]:
                fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
                roster_count = 0
                for pid in (fi.get("friendlyPlayers") or []):
                    a = actors.get(str(pid)) or actors.get(pid)
                    if a and a.get("name", "").lower() in KNOWN_ROSTER:
                        roster_count += 1
                if roster_count < MIN_ROSTER_PLAYERS:
                    below_threshold.append(fid)
            if below_threshold:
                rpt["mythic_fight_ids"] = [fid for fid in rpt["mythic_fight_ids"]
                                           if fid not in below_threshold]
                roster_removed += len(below_threshold)
                print(f"[{ts()}]   Roster filter {code}: removed {len(below_threshold)} fights "
                      f"(< {MIN_ROSTER_PLAYERS} roster players)")
        if roster_removed:
            total_after = sum(len(r["mythic_fight_ids"]) for r in reports_data.values())
            print(f"[{ts()}]   Roster filter complete: {roster_removed} fights removed, "
                  f"{total_after} remaining")
        else:
            print(f"[{ts()}]   Roster filter: all fights meet threshold ({MIN_ROSTER_PLAYERS}+ roster players)")
    elif MIN_ROSTER_PLAYERS > 0:
        if not ROSTER_LOCKED:
            print(f"[{ts()}]   Roster filter: min_roster_players={MIN_ROSTER_PLAYERS} but roster "
                  f"is unlocked — skipping filter")
        else:
            print(f"[{ts()}]   Roster filter: min_roster_players={MIN_ROSTER_PLAYERS} but roster.json "
                  f"has no characters — skipping filter")

    save_resume(3, _PHASE2_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 3: Rankings (DPS + HPS) for each mythic fight
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 3:
    phase_start = time.time()
    total_fights = sum(len(r["mythic_fight_ids"]) for r in reports_data.values())
    done_fights = sum(len(r["rankings"]) for r in reports_data.values())
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 3: Rankings — DPS + HPS ({total_fights} fights, {done_fights} already done)")
    print(f"{'='*70}")

    fight_num = done_fights
    for code, rpt in reports_data.items():
        mfights = rpt["mythic_fight_ids"]
        remaining = [fid for fid in mfights if str(fid) not in rpt["rankings"]]
        if not remaining:
            print(f"[{ts()}]   {code} ({rpt['date']}): {len(mfights)} fights — all ranked ✓")
            continue
        print(f"[{ts()}]   {code} ({rpt['date']}): {len(remaining)}/{len(mfights)} to rank")

        for bs in range(0, len(remaining), 3):
            batch = remaining[bs:bs+3]
            parts = []
            for fid in batch:
                parts.append('f%d_dps:rankings(fightIDs:[%d],playerMetric:dps,'
                             'compare:Parses,timeframe:Today)' % (fid, fid))
                parts.append('f%d_hps:rankings(fightIDs:[%d],playerMetric:hps,'
                             'compare:Parses,timeframe:Today)' % (fid, fid))

            query = '{reportData{r:report(code:"%s"){%s}}}' % (esc(code), ' '.join(parts))
            d = wcl_q(query)
            r_data = ((d.get("data") or {}).get("reportData") or {}).get("r") or {}

            for fid in batch:
                rpt["rankings"][str(fid)] = {
                    "dps": r_data.get(f"f{fid}_dps"),
                    "hps": r_data.get(f"f{fid}_hps"),
                }
                fight_num += 1
                fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
                boss = fi.get("name", "?")[:20]
                kill = "K" if fi.get("kill") else "W"
                player_count = 0
                for metric_key in [f"f{fid}_dps", f"f{fid}_hps"]:
                    md = r_data.get(metric_key)
                    if isinstance(md, dict) and "data" in md:
                        for entry in (md["data"] if isinstance(md["data"], list) else []):
                            for rk in ["tanks", "healers", "dps"]:
                                player_count += len(((entry.get("roles") or {}).get(rk) or {}).get("characters") or [])
                print(f"[{ts()}]     [{fight_num}/{total_fights}] {boss:20s} ({kill}) {player_count} ranked")

            if bs > 0 and bs % 6 == 0:
                save_resume(3, _PHASE3_VARS)

    print(f"\n[{ts()}] Phase 3 complete ({elapsed(phase_start)})")
    save_resume(4, _PHASE3_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 4: Deaths + Damage Taken + Boss Abilities + Consumables
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 4:
    phase_start = time.time()
    total_fights = sum(len(r["mythic_fight_ids"]) for r in reports_data.values())
    done_fights = sum(len(r["deaths"]) for r in reports_data.values())
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 4: Deaths + DamageTaken + Abilities + Consumables ({total_fights} fights, {done_fights} already done)")
    print(f"{'='*70}")
    print(f"[{ts()}]   Consumable filter: {CONSUMABLE_FILTER_EXPR}")

    fight_num = done_fights
    for code, rpt in reports_data.items():
        mfights = rpt["mythic_fight_ids"]
        remaining = [fid for fid in mfights if str(fid) not in rpt["deaths"]]
        if not remaining:
            print(f"[{ts()}]   {code} ({rpt['date']}): all done ✓")
            continue
        print(f"[{ts()}]   {code} ({rpt['date']}): {len(remaining)}/{len(mfights)} to process")

        for bs in range(0, len(remaining), 3):
            batch = remaining[bs:bs+3]
            parts = []
            for fid in batch:
                parts.append('f%d_de:table(dataType:Deaths,fightIDs:[%d])' % (fid, fid))
                parts.append('f%d_dt:table(dataType:DamageTaken,fightIDs:[%d])' % (fid, fid))
                parts.append('f%d_da:table(dataType:DamageTaken,fightIDs:[%d],hostilityType:Enemies,viewBy:Ability)' % (fid, fid))
                parts.append('f%d_cc:table(dataType:Casts,fightIDs:[%d],filterExpression:"%s",viewBy:Ability)' % (fid, fid, esc(CONSUMABLE_FILTER_EXPR)))

            query = '{reportData{r:report(code:"%s"){%s}}}' % (esc(code), ' '.join(parts))
            d = wcl_q(query)
            r_data = ((d.get("data") or {}).get("reportData") or {}).get("r") or {}

            for fid in batch:
                de_raw = r_data.get(f"f{fid}_de")
                dt_raw = r_data.get(f"f{fid}_dt")
                da_raw = r_data.get(f"f{fid}_da")
                cc_raw = r_data.get(f"f{fid}_cc")
                deaths = (((de_raw.get("data") or {}).get("entries") or [])
                          if isinstance(de_raw, dict) else [])
                dmg = (((dt_raw.get("data") or {}).get("entries") or [])
                       if isinstance(dt_raw, dict) else [])
                # Boss ability catalog: ability name/id/total damage + per-player sub-entries
                abilities = []
                if isinstance(da_raw, dict) and da_raw.get("data"):
                    for entry in (da_raw["data"].get("entries") or []):
                        # Keep full entry: may include subEntries/entries with per-player breakdowns
                        # from viewBy:Ability. Downstream mechanic scoring needs these.
                        ab = {
                            "name": entry.get("name", ""), "guid": entry.get("guid", 0),
                            "type": entry.get("type", ""), "total": entry.get("total", 0),
                        }
                        # Preserve per-player sub-breakdowns (key varies: subEntries, entries, etc.)
                        for sub_key in ("subEntries", "entries", "details"):
                            if entry.get(sub_key):
                                ab["subEntries"] = entry[sub_key]
                                break
                        abilities.append(ab)
                # Consumable casts: viewBy:Ability gives entries per spell,
                # subEntries per player who cast it
                consumes = []
                if isinstance(cc_raw, dict) and cc_raw.get("data"):
                    for entry in (cc_raw["data"].get("entries") or []):
                        ability_id = int(entry.get("guid", entry.get("id", 0)))
                        category = _CONSUMABLE_CATEGORY.get(
                            ability_id, "tempered_potion")
                        subs = (entry.get("subEntries")
                                or entry.get("entries")
                                or entry.get("details") or [])
                        if subs:
                            for sub in subs:
                                consumes.append({
                                    "name": sub.get("name", ""),
                                    "id": sub.get("id", 0),
                                    "type": sub.get("type", ""),
                                    "total": sub.get("total", 0),
                                    "category": category,
                                })
                        else:
                            # Fallback: no subEntries, entry is the ability itself
                            consumes.append({
                                "name": entry.get("name", ""),
                                "id": entry.get("id", 0),
                                "type": entry.get("type", ""),
                                "total": entry.get("total", 0),
                                "category": category,
                            })
                rpt["deaths"][str(fid)] = deaths
                rpt["damage_taken"][str(fid)] = dmg
                rpt["damage_taken_abilities"][str(fid)] = abilities
                rpt["consumable_casts"][str(fid)] = consumes
                fight_num += 1
                fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
                boss = fi.get("name", "?")[:20]
                kill = "K" if fi.get("kill") else "W"
                n_con = sum(c["total"] for c in consumes)
                print(f"[{ts()}]     [{fight_num}/{total_fights}] {boss:20s} ({kill}) {len(deaths)}d {len(abilities)}abil {n_con}con")

    print(f"\n[{ts()}] Phase 4 complete ({elapsed(phase_start)})")
    save_resume(5, _PHASE4_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 4.1: Mechanic-specific queries (interrupts + target swap + tank debuffs)
#  Only runs for encounters that need extra data per mechanic_rulesets.
#  Idempotent: checks if data already exists per fight before querying.
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 5:
    phase_start = time.time()

    # Build list of fights that need mechanic queries
    mechanic_queue = []  # (code, fid, encounter_id, needs_interrupts, target_mobs, debuff_names)
    for code, rpt in reports_data.items():
        for fid in rpt["mythic_fight_ids"]:
            fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
            eid = fi.get("encounterID", 0)
            needs_int = eid in MECHANIC_INTERRUPT_ENCOUNTERS
            target_mobs = MECHANIC_TARGET_SWAP.get(eid, [])
            debuff_names = TANK_SWAP_DEBUFFS.get(eid, [])

            if not needs_int and not target_mobs and not debuff_names:
                continue  # This encounter doesn't need extra queries

            # Check if already pulled (idempotent)
            sfid = str(fid)
            has_int = sfid in rpt.get("mechanic_interrupts", {})
            has_td = sfid in rpt.get("mechanic_target_damage", {})
            has_db = sfid in rpt.get("tank_swap_debuffs", {})
            still_needs_int = needs_int and not has_int
            still_needs_td = target_mobs and not has_td
            still_needs_db = debuff_names and not has_db
            if not still_needs_int and not still_needs_td and not still_needs_db:
                continue  # Already done

            mechanic_queue.append((code, fid, eid, still_needs_int,
                                   target_mobs if still_needs_td else [],
                                   debuff_names if still_needs_db else []))

    if not mechanic_queue:
        print(f"\n[{ts()}] PHASE 4.1: Mechanic queries — nothing to pull (all done or no encounters match)")
    else:
        print(f"\n{'='*70}")
        print(f"[{ts()}] PHASE 4.1: Mechanic-specific queries ({len(mechanic_queue)} fights)")
        print(f"{'='*70}")
        int_count = sum(1 for _, _, _, ni, _, _ in mechanic_queue if ni)
        ts_count = sum(1 for _, _, _, _, tm, _ in mechanic_queue if tm)
        db_count = sum(1 for _, _, _, _, _, db in mechanic_queue if db)
        print(f"[{ts()}]   Interrupt queries: {int_count} fights")
        print(f"[{ts()}]   Target-swap queries: {ts_count} fights")
        print(f"[{ts()}]   Tank debuff queries: {db_count} fights")

        # Group by report code for batching
        by_report = defaultdict(list)
        for item in mechanic_queue:
            by_report[item[0]].append(item)

        mech_num = 0
        for code, items in by_report.items():
            rpt = reports_data[code]
            print(f"\n[{ts()}]   Report {code} ({rpt['date']}): {len(items)} fights")

            # Batch up to 3 fights per query (each fight may have 1-4 aliases)
            for bs in range(0, len(items), 3):
                batch = items[bs:bs+3]
                parts = []
                # Track what we're querying so we can parse the response
                batch_meta = []  # (fid, alias_int, alias_td_list, alias_db)

                for code_b, fid, eid, needs_int, target_mobs, debuff_names in batch:
                    alias_int = None
                    alias_td = []
                    alias_db = None

                    if needs_int:
                        alias_int = f"f{fid}_int"
                        parts.append(f'{alias_int}:table(dataType:Interrupts,fightIDs:[{fid}])')

                    for mob_name in target_mobs:
                        safe_mob = re.sub(r'[^a-zA-Z0-9]', '', mob_name)[:15]
                        alias = f"f{fid}_td_{safe_mob}"
                        # filterExpression uses single quotes for string values
                        fe = "target.name='%s'" % mob_name.replace("'", "\\'")
                        parts.append(f'{alias}:table(dataType:DamageDone,'
                                     f'fightIDs:[{fid}],filterExpression:"{fe}")')
                        alias_td.append((alias, mob_name))

                    if debuff_names:
                        alias_db = f"f{fid}_db"
                        # Query Debuffs by ability name, viewBy:Ability for per-player breakdowns
                        db_fe = "ability.name in (%s)" % ",".join(
                            "'%s'" % n.replace("'", "\\'") for n in debuff_names)
                        parts.append(f'{alias_db}:table(dataType:Debuffs,'
                                     f'fightIDs:[{fid}],viewBy:Ability,'
                                     f'filterExpression:"{esc(db_fe)}")')

                    batch_meta.append((fid, alias_int, alias_td, alias_db))

                if not parts:
                    continue

                query = '{reportData{r:report(code:"%s"){%s}}}' % (esc(code), ' '.join(parts))
                d = wcl_q(query)
                r_data = ((d.get("data") or {}).get("reportData") or {}).get("r") or {}

                for fid, alias_int, alias_td, alias_db in batch_meta:
                    sfid = str(fid)
                    fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
                    boss = fi.get("name", "?")[:25]
                    mech_num += 1
                    log_parts = []

                    # Store interrupt data
                    if alias_int:
                        int_raw = r_data.get(alias_int, {})
                        int_entries = (((int_raw.get("data") or {}).get("entries") or [])
                                       if isinstance(int_raw, dict) else [])
                        rpt.setdefault("mechanic_interrupts", {})[sfid] = int_entries
                        log_parts.append(f"int={len(int_entries)}")

                    # Store target-swap damage data
                    if alias_td:
                        td_data = {}
                        for alias, mob_name in alias_td:
                            td_raw = r_data.get(alias, {})
                            td_entries = (((td_raw.get("data") or {}).get("entries") or [])
                                          if isinstance(td_raw, dict) else [])
                            td_data[mob_name] = td_entries
                            log_parts.append(f"{mob_name}={len(td_entries)}p")
                        rpt.setdefault("mechanic_target_damage", {})[sfid] = td_data

                    # Store tank swap debuff data
                    if alias_db:
                        db_raw = r_data.get(alias_db, {})
                        db_entries = []
                        if isinstance(db_raw, dict) and db_raw.get("data"):
                            for ability_entry in (db_raw["data"].get("entries") or []):
                                debuff_name = ability_entry.get("name", "")
                                # Per-player sub-entries (may be under subEntries, entries, or details)
                                sub = None
                                for sub_key in ("subEntries", "entries", "details"):
                                    if ability_entry.get(sub_key):
                                        sub = ability_entry[sub_key]
                                        break
                                if sub:
                                    for player_entry in sub:
                                        db_entries.append({
                                            "debuff_name": debuff_name,
                                            "debuff_guid": ability_entry.get("guid", 0),
                                            "player": player_entry.get("name", "?"),
                                            "player_class": player_entry.get("type", ""),
                                            "applications": player_entry.get("totalUses",
                                                            player_entry.get("uses",
                                                            player_entry.get("total", 0))),
                                            "uptime": player_entry.get("totalUptime",
                                                      player_entry.get("uptime", 0)),
                                        })
                                else:
                                    # No sub-entries — ability-level only (shouldn't happen with viewBy:Ability)
                                    db_entries.append({
                                        "debuff_name": debuff_name,
                                        "debuff_guid": ability_entry.get("guid", 0),
                                        "player": "?",
                                        "player_class": "",
                                        "applications": ability_entry.get("totalUses",
                                                        ability_entry.get("total", 0)),
                                        "uptime": 0,
                                    })
                        rpt.setdefault("tank_swap_debuffs", {})[sfid] = db_entries
                        n_apps = sum(e["applications"] for e in db_entries)
                        log_parts.append(f"debuffs={len(db_entries)}p/{n_apps}apps")

                    print(f"[{ts()}]     [{mech_num}/{len(mechanic_queue)}] {boss:25s} {' '.join(log_parts)}")

        print(f"\n[{ts()}] Phase 4.1 complete ({elapsed(phase_start)})")
    # No separate save_resume — Phase 4.1 is idempotent and will re-run if interrupted


# ══════════════════════════════════════════════════════════════════════
#  PHASE 4.5: Wipe DPS/HPS via table(DamageDone/Healing)
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 5:
    phase_start = time.time()
    total_wipes = 0
    total_remaining = 0
    for code, rpt in reports_data.items():
        wipe_fids = [f["id"] for f in rpt["fights"]
                     if f.get("difficulty") == 5 and (f.get("encounterID") or 0) > 0
                     and not f.get("kill")]
        already = set(rpt.get("wipe_dps", {}).keys())
        total_wipes += len(wipe_fids)
        total_remaining += len([fid for fid in wipe_fids if str(fid) not in already])

    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 4.5: Wipe DPS/HPS ({total_wipes} wipes, {total_remaining} remaining)")
    print(f"{'='*70}")

    if total_remaining == 0:
        print(f"[{ts()}]   All wipes already processed ✓")
    else:
        wipe_num = 0
        for code, rpt in reports_data.items():
            wipe_fids = [f["id"] for f in rpt["fights"]
                         if f.get("difficulty") == 5 and (f.get("encounterID") or 0) > 0
                         and not f.get("kill")]
            already = set(rpt.get("wipe_dps", {}).keys())
            remaining = [fid for fid in wipe_fids if str(fid) not in already]
            if not remaining:
                print(f"[{ts()}]   {code} ({rpt['date']}): no remaining wipes")
                continue
            print(f"[{ts()}]   {code} ({rpt['date']}): {len(remaining)} wipe fights")

            for bs in range(0, len(remaining), 3):
                batch = remaining[bs:bs+3]
                parts = []
                for fid in batch:
                    parts.append('f%d_dd:table(dataType:DamageDone,fightIDs:[%d])' % (fid, fid))
                    parts.append('f%d_hd:table(dataType:Healing,fightIDs:[%d])' % (fid, fid))

                query = '{reportData{r:report(code:"%s"){%s}}}' % (esc(code), ' '.join(parts))
                d = wcl_q(query)
                r_data = ((d.get("data") or {}).get("reportData") or {}).get("r") or {}

                for fid in batch:
                    dd_raw = r_data.get(f"f{fid}_dd")
                    hd_raw = r_data.get(f"f{fid}_hd")

                    dd_entries = []
                    if isinstance(dd_raw, dict) and dd_raw.get("data"):
                        dur_s = max((dd_raw["data"]).get("totalTime", 1) / 1000, 1)
                        for entry in (dd_raw["data"].get("entries") or []):
                            dd_entries.append({
                                "name": entry.get("name", ""), "type": entry.get("type", ""),
                                "icon": entry.get("icon", ""), "total": entry.get("total", 0),
                                "dps": round(entry.get("total", 0) / dur_s, 1),
                            })
                    rpt.setdefault("wipe_dps", {})[str(fid)] = dd_entries

                    hd_entries = []
                    if isinstance(hd_raw, dict) and hd_raw.get("data"):
                        dur_s = max((hd_raw["data"]).get("totalTime", 1) / 1000, 1)
                        for entry in (hd_raw["data"].get("entries") or []):
                            hd_entries.append({
                                "name": entry.get("name", ""), "type": entry.get("type", ""),
                                "icon": entry.get("icon", ""), "total": entry.get("total", 0),
                                "hps": round(entry.get("total", 0) / dur_s, 1),
                            })
                    rpt.setdefault("wipe_hps", {})[str(fid)] = hd_entries

                    wipe_num += 1
                    fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
                    boss = fi.get("name", "?")[:20]
                    pct = fi.get("bossPercentage", "?")
                    if isinstance(pct, (int, float)): pct = f"{pct/100:.1f}%"
                    print(f"[{ts()}]     [{wipe_num}/{total_remaining}] {boss:20s} (wipe→{pct}) {len(dd_entries)} dps, {len(hd_entries)} heal")

    print(f"\n[{ts()}] Phase 4.5 complete ({elapsed(phase_start)})")
    save_resume(6, _PHASE45_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 5: Build unique player list
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 6:
    phase_start = time.time()
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 5: Building player list")
    print(f"{'='*70}")

    player_info = {}
    for code, rpt in reports_data.items():
        actors = rpt["actors"]
        count = 0
        valid_fids = set(rpt.get("mythic_fight_ids", []))
        for f in rpt["fights"]:
            if f["id"] not in valid_fids:
                continue
            for pid in (f.get("friendlyPlayers") or []):
                a = actors.get(pid)
                if not a: continue
                name = a["name"]
                if name not in player_info or (not player_info[name].get("wcl_spec") and a.get("spec")):
                    player_info[name] = {
                        "wcl_class": a["class"], "wcl_spec": a["spec"],
                        "wcl_server": a["server"],
                        "realm_slug": to_realm_slug(a["server"]) if a["server"] else "",
                        "is_known": name.lower() in KNOWN_ROSTER,
                    }
                    count += 1
        print(f"[{ts()}]   {code} ({rpt['date']}): {count} new players")

    print(f"\n[{ts()}] Phase 5 complete: {len(player_info)} unique players ({elapsed(phase_start)})")
    for name in sorted(player_info.keys()):
        p = player_info[name]
        tag = " ★" if p["is_known"] else ""
        print(f"  {name:20s}  {p['wcl_class']:15s}  {p['wcl_spec']:15s}  {p['wcl_server']}{tag}")
    save_resume(7, _PHASE5_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 6: Blizzard API — profiles, equipment, encounters
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 8:
    phase_start = time.time()
    already_done = sum(1 for p in player_info.values() if "blizzard_profile" in p)
    remaining = [(n, p) for n, p in sorted(player_info.items()) if "blizzard_profile" not in p]

    # Only call Blizzard API for rostered players when locked — skip the rest with stubs
    roster_remaining = []
    non_roster_count = 0
    for n, p in remaining:
        if ROSTER_LOCKED and KNOWN_ROSTER and n.lower() not in KNOWN_ROSTER:
            p["blizzard_profile"] = {"error": "not_on_roster"}
            p["blizzard_equipment"] = {"error": "not_on_roster"}
            p["blizzard_encounters"] = {"error": "not_on_roster"}
            non_roster_count += 1
        else:
            roster_remaining.append((n, p))

    total = len(roster_remaining)
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 6: Blizzard API ({total} roster players, "
          f"{already_done} cached, {non_roster_count} non-roster skipped)")
    print(f"{'='*70}")

    if not bliz_token:
        print(f"[{ts()}]   ⚠ No Blizzard token — skipping all")
        for name, info in roster_remaining:
            info["blizzard_profile"] = {"error": "no_token"}
            info["blizzard_equipment"] = {"error": "no_token"}
            info["blizzard_encounters"] = {"error": "no_token"}
    else:
        for i, (name, info) in enumerate(roster_remaining):
            slug = info["realm_slug"]
            idx = already_done + i + 1
            name_lower = name.lower()
            if not slug:
                print(f"[{ts()}]   [{idx}/{total}] {name}: no realm → skip")
                info["blizzard_profile"] = {"error": "no_realm"}
                info["blizzard_equipment"] = {"error": "no_realm"}
                info["blizzard_encounters"] = {"error": "no_realm"}
                continue

            print(f"[{ts()}]   [{idx}/{total}] {name:20s} ({slug:15s}) profile...", end=" ", flush=True)
            profile_url = f"https://us.api.blizzard.com/profile/wow/character/{slug}/{name_lower}"
            prof = bliz_get(profile_url)
            info["blizzard_profile"] = prof
            ilvl = prof.get("equipped_item_level", "?") if "error" not in prof else "ERR"
            print(f"iLvl:{ilvl} gear...", end=" ", flush=True)

            equip = bliz_get(f"{profile_url}/equipment")
            info["blizzard_equipment"] = equip
            slots = len((equip.get("equipped_items") or [])) if "error" not in equip else 0
            print(f"{slots} slots", end=" ", flush=True)

            enc = bliz_get(f"{profile_url}/encounters/raids")
            info["blizzard_encounters"] = enc
            prog_str = "?"
            if "error" not in enc:
                for exp in (enc.get("expansions") or []):
                    for inst in (exp.get("instances") or []):
                        if "manaforge" in (inst.get("instance") or {}).get("name", "").lower():
                            for mode in (inst.get("modes") or []):
                                diff = (mode.get("difficulty") or {}).get("name", "")
                                p = mode.get("progress") or {}
                                if diff == "Mythic":
                                    prog_str = f"{p.get('completed_count',0)}/{p.get('total_count',0)} M"
            print(f"prog:{prog_str}")
            time.sleep(0.2)

            if (i + 1) % 10 == 0:
                save_resume(8, _PHASE7_VARS)

    print(f"\n[{ts()}] Phase 6 complete ({elapsed(phase_start)})")
    save_resume(9, _PHASE7_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 8: Guild zone rankings
# ══════════════════════════════════════════════════════════════════════
if resume_from <= 9:
    phase_start = time.time()
    print(f"\n{'='*70}")
    print(f"[{ts()}] PHASE 8: Guild zone rankings")
    print(f"{'='*70}")
    print(f"[{ts()}]   Querying {GUILD_NAME} / {SERVER_SLUG} / {SERVER_REGION}...")

    guild_rankings = None
    if GUILD_NAME and SERVER_SLUG:
        d = wcl_q('''
        {guildData{guild(name:"%s",serverSlug:"%s",serverRegion:"%s"){
          name id
          zoneRanking{
            progress{worldRank regionRank serverRank}
            speed{worldRank{number} regionRank{number} serverRank{number}
                  bestPerformanceAverage medianPerformanceAverage}
            completeRaidSpeed{worldRank{number} regionRank{number} serverRank{number}
                              bestPerformanceAverage completeRaidSpeed}
          }
        }}}''' % (GUILD_NAME, SERVER_SLUG, SERVER_REGION))

        gd = ((d.get("data") or {}).get("guildData") or {}).get("guild")
        if gd:
            guild_rankings = gd
            zr = gd.get("zoneRanking") or {}
            speed = zr.get("speed") or {}
            sr = (speed.get("serverRank") or {}).get("number", "?")
            rr = (speed.get("regionRank") or {}).get("number", "?")
            wr = (speed.get("worldRank") or {}).get("number", "?")
            print(f"[{ts()}]   Speed — Server:#{sr}  Region:#{rr}  World:#{wr}")
        else:
            print(f"[{ts()}]   WARNING: Guild not found in WCL")
    else:
        print(f"[{ts()}]   Skipping — no guild name or server in roster.json")

    print(f"\n[{ts()}] Phase 8 complete ({elapsed(phase_start)})")
    save_resume(10, _PHASE8_VARS)


# ══════════════════════════════════════════════════════════════════════
#  PHASE 8.5: Build pandas DataFrames from pulled data
#  Flattens nested JSON into tabular format for review + downstream use.
#  Outputs: raid_dataframes.xlsx  (all data + _dtypes schema sheet)
#           raid_dataframes.xlsx  (multi-sheet — single source of truth)
# ══════════════════════════════════════════════════════════════════════
phase_start = time.time()
print(f"\n{'='*70}")
print(f"[{ts()}] PHASE 8.5: Building pandas DataFrames")
print(f"{'='*70}")

import pandas as pd

DF_XLSX = "raid_dataframes.xlsx"

# ── Helper: get fight info ──
def fight_info(rpt, fid):
    fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
    dur_ms = fi.get("endTime", 0) - fi.get("startTime", 0)
    return {
        "report_code": rpt["code"],
        "date": rpt["date"],
        "fight_id": fid,
        "boss": fi.get("name", "?"),
        "encounter_id": fi.get("encounterID", 0),
        "kill": bool(fi.get("kill")),
        "duration_s": round(dur_ms / 1000, 1) if dur_ms > 0 else 0,
        "size": fi.get("size", 0),
        "boss_pct": fi.get("bossPercentage", None),
        "avg_ilvl": fi.get("averageItemLevel", None),
    }

# ── 1. FIGHTS TABLE ──
print(f"[{ts()}]   Building fights table...")
fights_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fights_rows.append(fight_info(rpt, fid))
df_fights = pd.DataFrame(fights_rows)
print(f"[{ts()}]     {len(df_fights)} fights")

# ── 2. PLAYER DAMAGE TAKEN (per player × ability × fight) ──
print(f"[{ts()}]   Building player_damage_taken table...")
pdt_rows = []
for code, rpt in reports_data.items():
    actors = rpt.get("actors", {})
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        dt_players = rpt.get("damage_taken", {}).get(str(fid), [])
        for p in dt_players:
            player_name = p.get("name", "?")
            player_class = p.get("type", "?")
            player_spec = (p.get("icon") or "").split("-", 1)[1] if "-" in (p.get("icon") or "") else ""
            for a in p.get("abilities", []):
                row = {
                    "report_code": fi_base["report_code"],
                    "date": fi_base["date"],
                    "fight_id": fid,
                    "boss": fi_base["boss"],
                    "encounter_id": fi_base["encounter_id"],
                    "kill": fi_base["kill"],
                    "player": player_name,
                    "player_class": player_class,
                    "player_spec": player_spec,
                    "ability_name": a.get("name", "?"),
                    "ability_total": a.get("total", 0),
                    "ability_type": a.get("type", 0),
                }
                pdt_rows.append(row)
df_player_dt = pd.DataFrame(pdt_rows)
print(f"[{ts()}]     {len(df_player_dt)} rows ({df_player_dt['player'].nunique() if len(df_player_dt) else 0} players)")

# ── 2b. PLAYER FIGHT SUMMARY (per player × fight totals) ──
print(f"[{ts()}]   Building player_fight_summary table...")
pfs_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        dt_players = rpt.get("damage_taken", {}).get(str(fid), [])
        for p in dt_players:
            pfs_rows.append({
                "report_code": fi_base["report_code"],
                "date": fi_base["date"],
                "fight_id": fid,
                "boss": fi_base["boss"],
                "encounter_id": fi_base["encounter_id"],
                "kill": fi_base["kill"],
                "player": p.get("name", "?"),
                "player_class": p.get("type", "?"),
                "guid": p.get("guid", 0),
                "total_damage_taken": p.get("total", 0),
                "total_damage_taken_reduced": p.get("totalReduced", 0),
                "active_time_ms": p.get("activeTime", 0),
                "overheal_received": p.get("overheal", 0),
                "item_level": p.get("itemLevel", None),
            })
df_fight_summary = pd.DataFrame(pfs_rows)
print(f"[{ts()}]     {len(df_fight_summary)} rows")

# ── 2c. PLAYER DAMAGE SOURCES (which mob/player damaged each player per fight) ──
print(f"[{ts()}]   Building player_damage_sources table...")
pds_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        dt_players = rpt.get("damage_taken", {}).get(str(fid), [])
        for p in dt_players:
            player_name = p.get("name", "?")
            for s in p.get("sources", []):
                pds_rows.append({
                    "report_code": fi_base["report_code"],
                    "date": fi_base["date"],
                    "fight_id": fid,
                    "boss": fi_base["boss"],
                    "encounter_id": fi_base["encounter_id"],
                    "kill": fi_base["kill"],
                    "player": player_name,
                    "source_name": s.get("name", "?"),
                    "source_type": s.get("type", "?"),
                    "source_total": s.get("total", 0),
                    "source_total_reduced": s.get("totalReduced", 0),
                })
df_damage_sources = pd.DataFrame(pds_rows)
print(f"[{ts()}]     {len(df_damage_sources)} rows")

# ── 3. PLAYER PERFORMANCE (DPS/HPS from rankings + wipe tables) ──
print(f"[{ts()}]   Building player_performance table...")
perf_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        sfid = str(fid)
        is_kill = fi_base["kill"]

        # Try rankings data for ALL fights (kills and wipes)
        rank = rpt.get("rankings", {}).get(sfid, {})
        got_rankings = False
        for metric in ["dps", "hps"]:
            rd = rank.get(metric)
            if not isinstance(rd, dict) or "data" not in rd:
                continue
            for entry in (rd["data"] if isinstance(rd["data"], list) else []):
                for role_key in ["tanks", "healers", "dps"]:
                    chars = ((entry.get("roles") or {}).get(role_key) or {}).get("characters") or []
                    for c in chars:
                        got_rankings = True
                        perf_rows.append({
                            "report_code": fi_base["report_code"],
                            "date": fi_base["date"],
                            "fight_id": fid,
                            "boss": fi_base["boss"],
                            "encounter_id": fi_base["encounter_id"],
                            "kill": is_kill,
                            "player": c.get("name", "?"),
                            "player_class": c.get("class", "?"),
                            "player_spec": c.get("spec", "?"),
                            "role": {"tanks": "Tank", "healers": "Healer", "dps": "DPS"}[role_key],
                            "metric": metric,
                            "amount": c.get("amount", 0),
                            "rank_percent": c.get("rankPercent", None),
                            "bracket_percent": c.get("bracketPercent", None),
                        })

        # Fallback: wipe table data if rankings unavailable for this fight
        if not got_rankings and not is_kill:
            for metric, wipe_key in [("dps", "wipe_dps"), ("hps", "wipe_hps")]:
                wipe_data = rpt.get(wipe_key, {}).get(sfid, [])
                for w in wipe_data:
                    perf_rows.append({
                        "report_code": fi_base["report_code"],
                        "date": fi_base["date"],
                        "fight_id": fid,
                        "boss": fi_base["boss"],
                        "encounter_id": fi_base["encounter_id"],
                        "kill": False,
                        "player": w.get("name", "?"),
                        "player_class": w.get("type", "?"),
                        "player_spec": (w.get("icon") or "").split("-", 1)[1] if "-" in (w.get("icon") or "") else "",
                        "role": "",
                        "metric": metric,
                        "amount": w.get(metric, w.get("total", 0)),
                        "rank_percent": None,
                        "bracket_percent": None,
                    })
df_perf = pd.DataFrame(perf_rows)
print(f"[{ts()}]     {len(df_perf)} rows")

# ── 4. DEATHS ──
print(f"[{ts()}]   Building deaths table...")
death_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        fight_deaths = rpt.get("deaths", {}).get(str(fid), [])
        # Deaths come sorted by timestamp from API
        for order, d in enumerate(fight_deaths, 1):
            kb = d.get("killingBlow") or {}
            death_rows.append({
                "report_code": fi_base["report_code"],
                "date": fi_base["date"],
                "fight_id": fid,
                "boss": fi_base["boss"],
                "encounter_id": fi_base["encounter_id"],
                "kill": fi_base["kill"],
                "player": d.get("name", "?"),
                "player_class": d.get("type", "?"),
                "death_order": order,
                "timestamp_ms": d.get("timestamp", 0),
                "killing_blow_name": kb.get("name", "?"),
                "killing_blow_guid": kb.get("guid", 0),
                "overkill": d.get("overkill", 0),
            })
df_deaths = pd.DataFrame(death_rows)
print(f"[{ts()}]     {len(df_deaths)} deaths")

# ── 4b. DEATH EVENTS (damage/healing sequence leading to each death) ──
print(f"[{ts()}]   Building death_events table...")
de_rows = []
for code, rpt in reports_data.items():
    actors = rpt.get("actors", {})
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        fight_deaths = rpt.get("deaths", {}).get(str(fid), [])
        for order, d in enumerate(fight_deaths, 1):
            player_name = d.get("name", "?")
            for e in d.get("events", []):
                # Resolve source actor name
                src_id = e.get("sourceID")
                src_actor = actors.get(str(src_id)) or actors.get(src_id) or {}
                ability = e.get("ability") or {}
                de_rows.append({
                    "report_code": fi_base["report_code"],
                    "date": fi_base["date"],
                    "fight_id": fid,
                    "boss": fi_base["boss"],
                    "kill": fi_base["kill"],
                    "player": player_name,
                    "death_order": order,
                    "event_type": e.get("type", "?"),
                    "timestamp_ms": e.get("timestamp", 0),
                    "source_name": src_actor.get("name", "?"),
                    "source_friendly": e.get("sourceIsFriendly", False),
                    "ability_name": ability.get("name", "?"),
                    "ability_guid": ability.get("guid", 0),
                    "amount": e.get("amount", 0),
                    "overkill": e.get("overkill", 0),
                    "mitigated": e.get("mitigated", 0),
                })
df_death_events = pd.DataFrame(de_rows)
print(f"[{ts()}]     {len(df_death_events)} events across {len(df_deaths)} deaths")

# ── 5. CONSUMABLES ──
print(f"[{ts()}]   Building consumables table...")
con_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        for c in rpt.get("consumable_casts", {}).get(str(fid), []):
            con_rows.append({
                "report_code": fi_base["report_code"],
                "date": fi_base["date"],
                "fight_id": fid,
                "boss": fi_base["boss"],
                "encounter_id": fi_base["encounter_id"],
                "kill": fi_base["kill"],
                "player": c.get("name", "?"),
                "player_class": c.get("type", "?"),
                "pot_count": c.get("total", 0),
                "category": c.get("category", "tempered_potion"),
            })
df_consumables = pd.DataFrame(con_rows)
print(f"[{ts()}]     {len(df_consumables)} rows")

# ── 6. MECHANIC TARGET DAMAGE (add priority / target swap) ──
print(f"[{ts()}]   Building mechanic_target_damage table...")
mtd_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        td = rpt.get("mechanic_target_damage", {}).get(str(fid), {})
        for mob_name, players in td.items():
            for p in players:
                mtd_rows.append({
                    "report_code": fi_base["report_code"],
                    "date": fi_base["date"],
                    "fight_id": fid,
                    "boss": fi_base["boss"],
                    "encounter_id": fi_base["encounter_id"],
                    "kill": fi_base["kill"],
                    "player": p.get("name", "?"),
                    "player_class": p.get("type", "?"),
                    "target_mob": mob_name,
                    "damage_done": p.get("total", 0),
                })
df_target_dmg = pd.DataFrame(mtd_rows)
print(f"[{ts()}]     {len(df_target_dmg)} rows")

# ── 7. MECHANIC INTERRUPTS ──
print(f"[{ts()}]   Building mechanic_interrupts table...")
mint_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        mi_raw = rpt.get("mechanic_interrupts", {}).get(str(fid), [])
        # Structure: [{"entries": [{ability with "details": [player entries]}]}]
        entries = []
        if mi_raw and isinstance(mi_raw, list):
            for wrapper in mi_raw:
                if isinstance(wrapper, dict):
                    entries.extend(wrapper.get("entries", []))
        for ability in entries:
            for p in ability.get("details", []):
                mint_rows.append({
                    "report_code": fi_base["report_code"],
                    "date": fi_base["date"],
                    "fight_id": fid,
                    "boss": fi_base["boss"],
                    "encounter_id": fi_base["encounter_id"],
                    "kill": fi_base["kill"],
                    "player": p.get("name", "?"),
                    "player_class": p.get("type", "?"),
                    "ability_interrupted": ability.get("name", "?"),
                    "ability_guid": ability.get("guid", 0),
                    "interrupts": p.get("total", 0),
                })
df_interrupts = pd.DataFrame(mint_rows)
print(f"[{ts()}]     {len(df_interrupts)} rows")

# ── 7b. TANK SWAP DEBUFFS ──
print(f"[{ts()}]   Building debuffs table...")
debuff_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        db_entries = rpt.get("tank_swap_debuffs", {}).get(str(fid), [])
        for e in db_entries:
            if e.get("player", "?") == "?":
                continue  # skip entries without a player
            debuff_rows.append({
                "report_code": fi_base["report_code"],
                "date": fi_base["date"],
                "fight_id": fid,
                "boss": fi_base["boss"],
                "encounter_id": fi_base["encounter_id"],
                "kill": fi_base["kill"],
                "player": e.get("player", "?"),
                "player_class": e.get("player_class", ""),
                "debuff_name": e.get("debuff_name", ""),
                "debuff_guid": e.get("debuff_guid", 0),
                "applications": e.get("applications", 0),
                "uptime": e.get("uptime", 0),
            })
df_debuffs = pd.DataFrame(debuff_rows)
print(f"[{ts()}]     {len(df_debuffs)} rows")

# ── 8. DAMAGE TAKEN ABILITIES (boss ability catalog per fight) ──
print(f"[{ts()}]   Building damage_taken_abilities table...")
dta_rows = []
for code, rpt in reports_data.items():
    for fid in rpt["mythic_fight_ids"]:
        fi_base = fight_info(rpt, fid)
        for a in rpt.get("damage_taken_abilities", {}).get(str(fid), []):
            dta_rows.append({
                "report_code": fi_base["report_code"],
                "date": fi_base["date"],
                "fight_id": fid,
                "boss": fi_base["boss"],
                "encounter_id": fi_base["encounter_id"],
                "kill": fi_base["kill"],
                "ability_name": a.get("name", "?"),
                "ability_guid": a.get("guid", 0),
                "ability_type": a.get("type", ""),
                "total_damage": a.get("total", 0),
            })
df_abilities = pd.DataFrame(dta_rows)
print(f"[{ts()}]     {len(df_abilities)} rows")

# ── Package all DataFrames ──

# ── 9. META (single-row table with pull config) ──
print(f"[{ts()}]   Building meta table...")
df_meta = pd.DataFrame([{
    "pull_timestamp": ts_full(),
    "guild_id": GUILD_ID, "guild_name": TEAM_NAME or GUILD_NAME,
    "parent_guild_id": PARENT_GUILD_ID or "", "parent_guild_name": GUILD_NAME,
    "guild_tag_id": GUILD_TAG_ID or "",
    "server": SERVER_SLUG, "region": SERVER_REGION,
    "zone_id": ZONE_ID, "zone_name": "Manaforge Omega",
    "partition_start_ms": PARTITION_START_MS,
    "partition_start_date": partition_date.strftime("%Y-%m-%d"),
    "partition_end_ms": PARTITION_END_MS or "",
    "partition_end_date": _end_display,
    "report_count": len(reports_data),
    "player_count": len(player_info),
    "wcl_queries": wcl_query_count,
    "trial_mode": TRIAL_MODE,
}])
print(f"[{ts()}]     1 row")

# ── 10. PLAYERS (one row per unique player — class, spec, Blizzard data) ──
print(f"[{ts()}]   Building players table...")
player_rows = []
for name, info in player_info.items():
    row = {
        "player": name,
        "wcl_class": info.get("wcl_class", ""),
        "wcl_spec": info.get("wcl_spec", ""),
        "wcl_server": info.get("wcl_server", ""),
        "realm_slug": info.get("realm_slug", ""),
        "is_known_roster": info.get("is_known", False),
    }
    # Blizzard profile data
    bp = info.get("blizzard_profile", {})
    if "error" not in bp:
        row["ilvl"] = bp.get("equipped_item_level", None)
        row["achievement_points"] = bp.get("achievement_points", None)
    else:
        row["ilvl"] = None
        row["achievement_points"] = None
    # Blizzard encounters — Manaforge progress
    enc = info.get("blizzard_encounters", {})
    if "error" not in enc:
        for exp in (enc.get("expansions") or []):
            for inst in (exp.get("instances") or []):
                if "manaforge" in (inst.get("instance") or {}).get("name", "").lower():
                    for mode in (inst.get("modes") or []):
                        diff = (mode.get("difficulty") or {}).get("name", "")
                        if "Mythic" in diff:
                            prog = mode.get("progress", {})
                            row["mythic_completed"] = prog.get("completed_count", 0)
                            row["mythic_total"] = prog.get("total_count", 0)
    row.setdefault("mythic_completed", None)
    row.setdefault("mythic_total", None)
    player_rows.append(row)
df_players = pd.DataFrame(player_rows)
print(f"[{ts()}]     {len(df_players)} players")

# ── 11. PLAYER EQUIPMENT (one row per player × gear slot — from Blizzard API) ──
print(f"[{ts()}]   Building player_equipment table...")
SKIP_SLOTS = {"SHIRT", "TABARD"}
ENCHANTABLE = {"CHEST", "LEGS", "FEET", "WRIST", "BACK", "FINGER_1", "FINGER_2", "MAIN_HAND"}
equip_rows = []
for name, info in player_info.items():
    be = info.get("blizzard_equipment", {})
    if "error" in be or not be:
        continue
    items = be.get("equipped_items") or []
    for it in items:
        slot = (it.get("slot") or {}).get("type", "")
        if slot in SKIP_SLOTS:
            continue
        ench_list = it.get("enchantments") or []
        sockets = it.get("sockets") or []
        equip_rows.append({
            "player": name,
            "slot": slot,
            "item_name": it.get("name", "?"),
            "item_ilvl": (it.get("level") or {}).get("value", None),
            "quality": (it.get("quality") or {}).get("name", ""),
            "enchant": ench_list[0].get("display_string", "") if ench_list else "",
            "needs_enchant": slot in ENCHANTABLE and not ench_list,
            "gems": sum(1 for s in sockets if "item" in s),
            "empty_sockets": sum(1 for s in sockets if "item" not in s),
        })
df_equipment = pd.DataFrame(equip_rows)
print(f"[{ts()}]     {len(df_equipment)} rows ({df_equipment['player'].nunique() if len(df_equipment) else 0} players)")

# ── 12. ACTORS (per-report player ID → name/class/spec mapping) ──
print(f"[{ts()}]   Building actors table...")
actor_rows = []
for code, rpt in reports_data.items():
    for aid, a in rpt.get("actors", {}).items():
        actor_rows.append({
            "report_code": code,
            "actor_id": int(aid) if str(aid).isdigit() else aid,
            "name": a.get("name", "?"),
            "player_class": a.get("class", ""),
            "player_spec": a.get("spec", ""),
            "server": a.get("server", ""),
        })
df_actors = pd.DataFrame(actor_rows)
print(f"[{ts()}]     {len(df_actors)} rows")

# ── 13. FIGHT ROSTER (which players were in each fight) ──
print(f"[{ts()}]   Building fight_roster table...")
roster_rows = []
for code, rpt in reports_data.items():
    actors = rpt.get("actors", {})
    for fid in rpt["mythic_fight_ids"]:
        fi = next((f for f in rpt["fights"] if f["id"] == fid), {})
        for pid in (fi.get("friendlyPlayers") or []):
            a = actors.get(str(pid)) or actors.get(pid)
            if a:
                roster_rows.append({
                    "report_code": code,
                    "date": rpt["date"],
                    "fight_id": fid,
                    "boss": fi.get("name", "?"),
                    "encounter_id": fi.get("encounterID", 0),
                    "kill": bool(fi.get("kill")),
                    "player": a.get("name", "?"),
                    "player_class": a.get("class", ""),
                    "player_spec": a.get("spec", ""),
                    "actor_id": pid,
                })
df_fight_roster = pd.DataFrame(roster_rows)
print(f"[{ts()}]     {len(df_fight_roster)} rows")

# ── 14. GUILD RANKINGS ──
print(f"[{ts()}]   Building guild_rankings table...")
gr_rows = []
zr = (guild_rankings.get("zoneRanking") or {}) if guild_rankings else {}
for metric_key in ["progress", "speed", "completeRaidSpeed"]:
    md = zr.get(metric_key)
    if not isinstance(md, dict):
        continue
    row = {"metric": metric_key}
    for rank_type in ["worldRank", "regionRank", "serverRank"]:
        rv = md.get(rank_type)
        if isinstance(rv, dict):
            row[rank_type] = rv.get("number", None)
        elif isinstance(rv, (int, float)):
            row[rank_type] = rv
        else:
            row[rank_type] = None
    for extra_key in ["bestPerformanceAverage", "medianPerformanceAverage", "completeRaidSpeed"]:
        if extra_key in md:
            row[extra_key] = md[extra_key]
    gr_rows.append(row)
df_guild_rankings = pd.DataFrame(gr_rows) if gr_rows else pd.DataFrame(columns=["metric"])
print(f"[{ts()}]     {len(df_guild_rankings)} rows")

dataframes = {
    "meta": df_meta,
    "fights": df_fights,
    "fight_roster": df_fight_roster,
    "players": df_players,
    "player_equipment": df_equipment,
    "player_fight_summary": df_fight_summary,
    "player_damage_taken": df_player_dt,
    "player_damage_sources": df_damage_sources,
    "player_performance": df_perf,
    "deaths": df_deaths,
    "death_events": df_death_events,
    "consumables": df_consumables,
    "mechanic_target_damage": df_target_dmg,
    "mechanic_interrupts": df_interrupts,
    "debuffs": df_debuffs,
    "damage_taken_abilities": df_abilities,
    "actors": df_actors,
    "guild_rankings": df_guild_rankings,
}

# ── Save xlsx (single source of truth) ──
# Includes a _dtypes sheet so downstream scripts can restore exact types on load.
print(f"\n[{ts()}]   Saving {DF_XLSX}...")

# Build dtype registry: sheet → column → dtype string
dtype_rows = []
for name, df in dataframes.items():
    for col in df.columns:
        dtype_rows.append({
            "sheet": name[:31],
            "column": col,
            "dtype": str(df[col].dtype),
        })
df_dtypes = pd.DataFrame(dtype_rows)

with pd.ExcelWriter(DF_XLSX, engine="openpyxl") as writer:
    # Write _dtypes sheet first (schema reference)
    df_dtypes.to_excel(writer, sheet_name="_dtypes", index=False)
    # Write data sheets — na_rep="NA" so nulls are explicit, not blank cells
    for name, df in dataframes.items():
        sheet = name[:31]
        if len(df) == 0:
            df.to_excel(writer, sheet_name=sheet, index=False, na_rep="NA")
        elif len(df) > 1000000:
            print(f"[{ts()}]     ⚠ {name}: {len(df)} rows, truncating to 1M for xlsx")
            df.head(1000000).to_excel(writer, sheet_name=sheet, index=False, na_rep="NA")
        else:
            df.to_excel(writer, sheet_name=sheet, index=False, na_rep="NA")

xlsx_size = os.path.getsize(DF_XLSX) / (1024 * 1024)
print(f"[{ts()}]     ✓ {DF_XLSX} ({xlsx_size:.1f} MB)")

# ── Summary ──
print(f"\n[{ts()}]   DataFrame summary:")
for name, df in dataframes.items():
    print(f"    {name:30s} {len(df):>7,} rows × {len(df.columns):>2} cols")

print(f"\n[{ts()}] Phase 8.5 complete ({elapsed(phase_start)})")


# ══════════════════════════════════════════════════════════════════════
#  PHASE 9: Roster update + cleanup
# ══════════════════════════════════════════════════════════════════════
phase_start = time.time()
print(f"\n{'='*70}")
print(f"[{ts()}] PHASE 9: Roster update + cleanup")
print(f"{'='*70}")

print(f"[{ts()}]   Checking WCL rate limit...")
pts = wcl_q("{rateLimitData{pointsSpentThisHour}}")
points_used = ((pts.get("data") or {}).get("rateLimitData") or {}).get("pointsSpentThisHour", "?")
print(f"[{ts()}]   WCL points used this hour: {points_used}")

# ── Roster update ──
print(f"[{ts()}]   Updating {ROSTER_FILE}...")
roster_data = {"meta": {}, "players": {}, "unlinked": []}
if os.path.exists(ROSTER_FILE):
    roster_data = _read_json(ROSTER_FILE)
    mains = sum(len(p.get("mains", [])) for p in roster_data.get("players", {}).values())
    alts = sum(len(p.get("alts", [])) for p in roster_data.get("players", {}).values())
    unl = len(roster_data.get("unlinked", []))
    print(f"[{ts()}]   Existing roster: {mains} mains, {alts} alts, {unl} unlinked")

# Always sync team identity from config.json into roster
roster_data["team"] = {
    "guild_name": GUILD_NAME,
    "team_name": TEAM_NAME,
    "server": _server_raw,
    "region": SERVER_REGION,
}

known_chars = set()
for pdata in roster_data.get("players", {}).values():
    for c in pdata.get("mains", []): known_chars.add(c)
    for c in pdata.get("alts", []): known_chars.add(c)
for c in roster_data.get("unlinked", []): known_chars.add(c)
for c in roster_data.get("excluded", []): known_chars.add(c)

# Migrate any existing unlinked chars → player entries (one-time upgrade)
players = roster_data.get("players", {})
unlinked = roster_data.get("unlinked", [])
if unlinked:
    migrated = 0
    for c in unlinked:
        if c not in players:
            players[c] = {"mains": [], "alts": [c]}
            migrated += 1
    if migrated:
        print(f"[{ts()}]     Migrated {migrated} unlinked → player entries")
    roster_data["unlinked"] = []
    roster_data["players"] = players

# Add new characters as player entries (player_name = char_name, listed as alt)
new_chars = [c for c in player_info if c not in known_chars]
if new_chars:
    for c in sorted(new_chars):
        if c not in players:
            players[c] = {"mains": [], "alts": [c]}
            print(f"[{ts()}]     NEW: {c} → player entry (alt)")
    roster_data["players"] = players

roster_data.setdefault("meta", {})["last_updated"] = ts_full()
with open(ROSTER_FILE, "w", encoding="utf-8") as f:
    json.dump(roster_data, f, indent=2, ensure_ascii=False)
if new_chars:
    print(f"[{ts()}]   Saved {ROSTER_FILE} — {len(new_chars)} new character(s)")
else:
    print(f"[{ts()}]   Saved {ROSTER_FILE} — team identity synced, no new characters")

# ── Cleanup ──
if os.path.exists(RESUME_PKL):
    os.remove(RESUME_PKL)
    print(f"[{ts()}]   Removed {RESUME_PKL} (complete run — no resume needed)")

# ── Summary ──
print(f"\n{'='*70}")
print(f"[{ts()}] ✅ PULL COMPLETE")
print(f"{'='*70}")
print(f"  Output:      {DF_XLSX} ({xlsx_size:.1f} MB)")
print(f"  Date range:  {partition_date.strftime('%Y-%m-%d')} → {_end_display}")
print(f"  Reports:     {len(reports_data)} raid nights")
print(f"  Players:     {len(player_info)} unique characters")
print(f"  WCL queries: {wcl_query_count}")
print(f"  WCL points:  {points_used}")
print(f"  Total time:  {elapsed(api_start_time)}")
if TRIAL_MODE:
    print(f"  ⚡ TRIAL MODE — only first report was processed")
