"""
WCL Probe — Scans top-ranked raid reports to catalogue boss abilities.
Uses Blizzard Dungeon Journal as the authoritative ability whitelist.

Usage:  python wcl_probe.py
"""

import json, requests, time, os, sys, re
from datetime import datetime, timezone

# Force UTF-8 output regardless of Windows console encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Paths ──
if getattr(sys, "frozen", False):
    SCRIPT_DIR = os.path.dirname(sys.executable)
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")
CACHE_FILE = os.path.join(SCRIPT_DIR, "probe_cache.json")
MECHANIC_FILE = os.path.join(SCRIPT_DIR, "mechanic_rulesets.json")


# ═══════════════════════════════════════════════════════════════
#  Config + Auth
# ═══════════════════════════════════════════════════════════════

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


def load_config():
    return _read_json(CONFIG_FILE)


def wcl_auth(config):
    wcl = config["warcraftlogs"]
    resp = requests.post(
        "https://www.warcraftlogs.com/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(wcl["client_id"], wcl["client_secret"]),
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def bliz_auth(config):
    bliz = config["blizzard"]
    resp = requests.post(
        "https://oauth.battle.net/token",
        data={"grant_type": "client_credentials"},
        auth=(bliz["client_id"], bliz["client_secret"]),
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


# ═══════════════════════════════════════════════════════════════
#  WCL query helper
# ═══════════════════════════════════════════════════════════════

WCL_URL = "https://www.warcraftlogs.com/api/v2/client"
wcl_headers = None
query_count = 0


def wcl_q(query):
    global query_count
    time.sleep(0.07)
    for attempt in range(5):
        try:
            r = requests.post(WCL_URL, headers=wcl_headers,
                              json={"query": query}, timeout=120)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10 * (attempt + 1)))
                print(f"  Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code in (502, 504):
                wait = (attempt + 1) * 5
                print(f"  WCL {r.status_code} — retry in {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            query_count += 1
            return r.json()
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError):
            if attempt < 4:
                wait = (attempt + 1) * 3
                print(f"  Timeout — retry in {wait}s...")
                time.sleep(wait)
                continue
            raise
    return {}


# ═══════════════════════════════════════════════════════════════
#  Blizzard spell lookup
# ═══════════════════════════════════════════════════════════════

bliz_headers = None


def _auto_short_name(name):
    """Generate a short display name from a boss name.
    'Dimensius, the All-Devouring' -> 'Dimensius'
    'The Soul Hunters' -> 'Soul Hunters'
    'Nexus-King Salhadaar' -> 'Nexus-King'
    'Plexus Sentinel' -> 'Plexus'
    'Loom\\'ithar' -> 'Loom\\'ithar' (single word kept)"""
    # Strip after comma
    if "," in name:
        name = name.split(",")[0].strip()
    # Strip leading "The " and keep remainder as-is
    if name.startswith("The "):
        return name[4:]
    words = name.split()
    if len(words) <= 1:
        return name
    first = words[0]
    # Hyphenated first word is distinctive enough (Nexus-King)
    if "-" in first:
        return first
    return first


def bliz_get(path, region="us"):
    """Generic Blizzard API GET request."""
    url = (f"https://{region}.api.blizzard.com{path}"
           f"{'&' if '?' in path else '?'}"
           f"namespace=static-{region}&locale=en_US")
    time.sleep(0.05)
    r = requests.get(url, headers=bliz_headers, timeout=15)
    if r.status_code != 200:
        return None
    return r.json()


def bliz_find_instance(raid_name, region="us"):
    """Search the journal-instance index for a raid by name.
    Returns the instance ID or None."""
    data = bliz_get("/data/wow/journal-instance/index", region)
    if not data:
        return None
    for inst in data.get("instances", []):
        if inst.get("name", "").lower() == raid_name.lower():
            return inst["id"]
    # Partial match fallback
    for inst in data.get("instances", []):
        if raid_name.lower() in inst.get("name", "").lower():
            return inst["id"]
    return None


def bliz_instance_encounters(instance_id, region="us"):
    """Fetch all encounter IDs from a journal instance.
    Returns [(journal_id, name), ...]."""
    data = bliz_get(f"/data/wow/journal-instance/{instance_id}", region)
    if not data:
        return []
    return [(e["id"], e.get("name", "?"))
            for e in data.get("encounters", [])]


def bliz_journal_encounter(encounter_id, region="us"):
    """Fetch Dungeon Journal data for an encounter from Blizzard API.
    Returns (id_descs, name_descs, all_names, creature_names) where:
      id_descs = {spell_id: description} (only with real body_text)
      name_descs = {spell_name: description} (only with real body_text)
      all_names = set of all spell names (for whitelist, always populated)
      creature_names = set of all creature/NPC names (boss + adds)"""
    data = bliz_get(f"/data/wow/journal-encounter/{encounter_id}", region)
    if not data:
        return {}, {}, set(), set()
    id_descs = {}    # {spell_id(int): description}
    name_descs = {}  # {spell_name(str): description}
    all_names = set()
    creature_names = set()
    _extract_journal_spells(data.get("sections", []),
                            id_descs, name_descs, all_names, "")
    # Extract creature/NPC names (boss + adds)
    for creature in data.get("creatures", []):
        # Structure varies: {"creature": {"name": "X"}} or {"name": "X"}
        cname = ""
        if isinstance(creature.get("creature"), dict):
            cname = creature["creature"].get("name", "")
        if not cname:
            cname = creature.get("name", "")
        if cname:
            creature_names.add(cname)
    return id_descs, name_descs, all_names, creature_names


def _extract_journal_spells(sections, id_descs, name_descs, all_names,
                            parent_ctx):
    """Recursively walk journal sections, extracting spell names + descriptions.
    all_names: always populated (for whitelist).
    id_descs/name_descs: only populated when real body_text exists."""
    for section in sections:
        spell = section.get("spell")
        title = section.get("title", "")
        body = (section.get("body_text") or section.get("body") or "").strip()

        # Build context chain for child sections
        ctx = f"{parent_ctx} > {title}" if parent_ctx else title

        if spell:
            sid = spell.get("id")
            sname = spell.get("name", "")

            # Always add to whitelist
            if sname:
                all_names.add(sname)

            # Only store descriptions when there's actual body_text
            if body:
                clean = body.replace("<br/>", "\n").replace("<br>", "\n")
                clean = re.sub(r"<[^>]+>", "", clean).strip()
                clean = clean.replace("$bullet;", "•")
                if sid and sid not in id_descs:
                    id_descs[sid] = clean
                if sname and sname not in name_descs:
                    name_descs[sname] = clean

        _extract_journal_spells(section.get("sections", []),
                                id_descs, name_descs, all_names, ctx)


# ═══════════════════════════════════════════════════════════════
#  Main probe
# ═══════════════════════════════════════════════════════════════

def main():
    global wcl_headers, bliz_headers, query_count

    print("=" * 60)
    print("  WCL PROBE — Ability Scanner")
    print("=" * 60)
    probe_start = time.time()

    # ── Load config & auth ──
    print("\n[Step 1/6] Loading config.json...")
    config = load_config()
    region = config.get("blizzard", {}).get("region", "us")
    print("  Config loaded.")

    print("\n[Step 2/6] Authenticating with APIs...")
    print("  Connecting to WCL...")
    wcl_headers = wcl_auth(config)
    print("  WCL authenticated.")
    print("  Connecting to Blizzard...")
    bliz_headers = bliz_auth(config)
    print("  Blizzard authenticated.")

    # ── Build list of enabled raids ──
    raids_cfg = config.get("raids", [])
    enabled_raids = [r["name"] for r in raids_cfg if r.get("enabled")]
    if not enabled_raids:
        # Backward compat: old current_raid field
        cr = config.get("current_raid", "")
        if cr:
            enabled_raids = [cr]

    if enabled_raids:
        print(f"\n  Enabled raids: {', '.join(enabled_raids)}")
    else:
        print("\n  No raids configured — will auto-detect.")

    # ── Discover all raid zones from WCL ──
    print("\n[Step 1/4] Discovering raid zones...")
    print("  Querying WCL worldData...")
    zone_data = wcl_q(
        '{worldData{expansions{id name zones{id name encounters{id name}}}}}'
    )

    all_raid_zones = []
    for exp in zone_data.get("data", {}).get("worldData", {}).get("expansions", []):
        for z in exp.get("zones", []):
            if "mythic+" not in z["name"].lower() and z.get("encounters"):
                all_raid_zones.append(z)
    print(f"  Found {len(all_raid_zones)} raid zones total.")

    # Match enabled raids to WCL zones (or auto-detect if none configured)
    matched_zones = []
    if enabled_raids:
        for raid_name in enabled_raids:
            found = None
            for z in all_raid_zones:
                if z["name"].lower() == raid_name.lower():
                    found = z
                    break
            if found:
                matched_zones.append(found)
                print(f"  Matched: {found['name']} (ID {found['id']})")
            else:
                print(f"  WARNING: \"{raid_name}\" not found in WCL zones — skipping")
    else:
        auto = max(all_raid_zones, key=lambda z: z["id"])
        matched_zones.append(auto)
        print(f"  Auto-detected: {auto['name']} (ID {auto['id']})")

    if not matched_zones:
        print("\nERROR: No raid zones matched! Check your config.json raids list.")
        return

    # ── Accumulated state across all raids ──
    cache = {
        "probe_date": datetime.now(tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"),
        "zones": [],
        "bosses": {},
        "spell_descriptions": {},
        "spell_name_descriptions": {},
    }
    all_journal_names = set()
    all_id_descriptions = {}
    all_name_descriptions = {}
    # For mechanic_rulesets: {raid_name: [boss_names]}
    raid_boss_orders = {}

    # ══════════════════════════════════════════════════════════════
    #  Per-raid loop
    # ══════════════════════════════════════════════════════════════
    total_raids = len(matched_zones)
    for ri, raid_zone in enumerate(matched_zones, 1):
        raid_name = raid_zone["name"]
        encounters = raid_zone.get("encounters", [])

        print(f"\n{'=' * 60}")
        print(f"  RAID {ri}/{total_raids}: {raid_name}")
        print(f"  Bosses ({len(encounters)}):")
        for e in encounters:
            print(f"    - {e['name']} (encounter ID {e['id']})")
        print(f"{'=' * 60}")

        cache["zones"].append({"id": raid_zone["id"], "name": raid_name})

        # ── Partition ──
        partition_override = config.get("raid_partition", None)
        if partition_override:
            partition = int(partition_override)
            print(f"\n  Partition override from config: {partition}")
        else:
            print("\n  Fetching partition info...")
            part_data = wcl_q(
                '{worldData{zone(id:%d){partitions{id name default}}}}' % raid_zone["id"]
            )
            parts = (part_data.get("data") or {}).get("worldData", {}).get("zone", {}).get("partitions", [])
            partition = max((p["id"] for p in parts), default=None) if parts else None
            if partition:
                pname = next((p["name"] for p in parts if p["id"] == partition), "?")
                print(f"  Using partition: {partition} ({pname})")
            else:
                print("  WARNING: No partitions found, proceeding without filter.")

        # ── Fetch Dungeon Journal — build ability whitelist ──
        print(f"\n[Step 2/4] Fetching Dungeon Journal for \"{raid_name}\"...")

        print(f"  Searching journal-instance index...", end=" ", flush=True)
        inst_id = bliz_find_instance(raid_name, region)
        if not inst_id:
            print("NOT FOUND")
            print(f"  FATAL: Cannot find \"{raid_name}\" in Blizzard journal.")
            print(f"  The journal whitelist is required to filter abilities.")
            continue
        print(f"instance ID {inst_id}")

        print(f"  Fetching encounters from instance...", end=" ", flush=True)
        journal_encs = bliz_instance_encounters(inst_id, region)
        print(f"{len(journal_encs)} bosses")

        # Build whitelist and descriptions for this raid — PER BOSS
        per_boss_journal = {}   # {boss_name: set_of_ability_names}
        per_boss_creatures = {} # {boss_name: set_of_creature_names}
        id_descriptions = {}
        name_descriptions = {}
        journal_names = set()   # zone-wide for stats display only

        # Map journal encounter names to WCL encounter names (case-insensitive)
        wcl_name_map = {e["name"].lower(): e["name"] for e in encounters}

        for j_id, j_name in journal_encs:
            print(f"    [{j_name}] (journal ID {j_id})...", end=" ", flush=True)
            j_id_descs, j_name_descs, j_all_names, j_creatures = (
                bliz_journal_encounter(j_id, region))
            if j_id_descs or j_name_descs or j_all_names:
                for sid, desc in j_id_descs.items():
                    id_descriptions[str(sid)] = desc
                name_descriptions.update(j_name_descs)
                journal_names.update(j_all_names)
                # Map this journal encounter to the WCL boss name
                wcl_boss = wcl_name_map.get(j_name.lower(), j_name)
                per_boss_journal[wcl_boss] = j_all_names
                per_boss_creatures[wcl_boss] = j_creatures
                parts = [f"{len(j_all_names)} abilities"]
                if j_creatures:
                    parts.append(f"{len(j_creatures)} creatures")
                print(f"{', '.join(parts)}")
            else:
                print("no data")

        print(f"\n  Journal whitelist: {len(journal_names)} unique ability names")
        print(f"  Descriptions: {len(id_descriptions)} by ID, "
              f"{len(name_descriptions)} by name")

        # Accumulate into global sets
        all_journal_names.update(journal_names)
        all_id_descriptions.update(id_descriptions)
        all_name_descriptions.update(name_descriptions)

        # ── For each boss, grab top report codes for Heroic + Mythic ──
        difficulties = [(5, "Mythic"), (4, "Heroic")]
        print(f"\n[Step 3/4] Finding top-ranked reports "
              f"({len(encounters)} bosses x {len(difficulties)} difficulties)...")
        boss_reports = {}

        for diff_id, diff_name in difficulties:
            print(f"\n  -- {diff_name} (difficulty {diff_id}) --")
            for i, enc in enumerate(encounters, 1):
                print(f"  [{i}/{len(encounters)}] {enc['name']}...", end=" ",
                      flush=True)
                part_str = f"partition:{partition}," if partition else ""
                q = ('{worldData{encounter(id:%d){characterRankings('
                     'className:"Warrior",specName:"Arms",difficulty:%d,'
                     'metric:dps,%sserverRegion:"US",page:1)}}}' % (
                         enc["id"], diff_id, part_str))
                data = wcl_q(q)
                cr = ((data.get("data") or {}).get("worldData", {})
                      .get("encounter", {}).get("characterRankings", {}))
                rankings = cr.get("rankings", [])

                if rankings:
                    report = rankings[0].get("report", {})
                    code = report.get("code", "")
                    fight_id = report.get("fightID", 0)
                    if code:
                        if enc["id"] not in boss_reports:
                            boss_reports[enc["id"]] = {
                                "name": enc["name"], "reports": []}
                        boss_reports[enc["id"]]["reports"].append(
                            (code, fight_id, diff_name))
                        print(f"report {code} (fight {fight_id})")
                    else:
                        print("no report code found")
                else:
                    print(f"no {diff_name} rankings found")

        if not boss_reports:
            print(f"\n  WARNING: No reports found for {raid_name} — skipping.")
            continue

        total_reports = sum(len(b["reports"]) for b in boss_reports.values())
        print(f"\n  Found {total_reports} reports across "
              f"{len(boss_reports)}/{len(encounters)} bosses.")

        # ── Fetch abilities per boss from reports, filtered by journal ──
        print(f"\n[Step 4/4] Scanning reports for abilities "
              f"(filtered by journal whitelist)...")

        boss_order_this_raid = []

        for enc_id, info in boss_reports.items():
            boss_name = info["name"]
            boss_order_this_raid.append(boss_name)
            print(f"\n  {boss_name}")

            # Per-boss journal whitelist (not zone-wide)
            boss_journal = per_boss_journal.get(boss_name, set())
            boss_creatures = per_boss_creatures.get(boss_name, set())
            if not boss_journal:
                print(f"    WARNING: No journal whitelist for {boss_name} — "
                      f"using zone-wide fallback")
                boss_journal = journal_names
            else:
                parts = [f"{len(boss_journal)} abilities"]
                if boss_creatures:
                    parts.append(f"{len(boss_creatures)} creatures")
                print(f"    Journal whitelist: {', '.join(parts)}")
                if boss_creatures:
                    for cname in sorted(boss_creatures):
                        tag = " <-- BOSS" if cname.lower() == boss_name.lower() else ""
                        print(f"      creature: {cname}{tag}")

            merged_dt = {}   # {ability_name: {name, gameIDs, total, players: set()}}
            merged_db = {}   # {name: {name, gameIDs}}
            merged_adds = {} # {enemy_name: total_damage_taken}
            max_raid_size = 0
            skipped_dt = set()
            skipped_db = set()

            for code, fid, diff_name in info["reports"]:
                print(f"    {diff_name}: report {code}, fight {fid}...",
                      end=" ", flush=True)

                # Three queries:
                #   dt = DamageTaken per player (each entry has .abilities[])
                #   dd = DamageDone viewBy:Target (each entry = enemy mob)
                #   db = Debuffs (auras applied to players)
                q = ('{reportData{report(code:"%s"){'
                     'fights(fightIDs:[%d]){id size friendlyPlayers}'
                     'masterData{abilities{gameID name icon type}}'
                     'dt:table(dataType:DamageTaken,fightIDs:[%d])'
                     'dd:table(dataType:DamageDone,fightIDs:[%d],viewBy:Target)'
                     'db:table(dataType:Debuffs,fightIDs:[%d])'
                     '}}}' % (code, fid, fid, fid, fid))

                resp = wcl_q(q)
                print("done.")
                report = ((resp.get("data") or {}).get("reportData", {})
                          .get("report", {}))

                # ── Raid size from fight.size or friendlyPlayers ──
                fights = report.get("fights") or []
                for fight in fights:
                    size = fight.get("size") or len(
                        fight.get("friendlyPlayers") or [])
                    if size > max_raid_size:
                        max_raid_size = size

                master_abilities = report.get("masterData", {}).get(
                    "abilities", [])
                ability_lookup = {}
                for a in master_abilities:
                    gid = int(a.get("gameID", 0))
                    if gid:
                        ability_lookup[gid] = {
                            "name": a.get("name", "Unknown"),
                            "icon": a.get("icon", ""),
                            "type": a.get("type", ""),
                        }

                # ── Parse dt: per-player DamageTaken ──
                # Each entry = a player; entry.abilities[] = what hit them
                dt_entries = ((report.get("dt") or {}).get("data", {})
                              .get("entries", []))
                for player in dt_entries:
                    player_name = player.get("name", "")
                    if not player_name:
                        continue
                    for a in player.get("abilities", []):
                        name = a.get("name", "")
                        gid = int(a.get("guid", a.get("id", 0)))
                        total = a.get("total", 0)
                        if not name or total <= 0:
                            continue
                        if name not in boss_journal:
                            skipped_dt.add(name)
                            continue
                        if name not in merged_dt:
                            merged_dt[name] = {
                                "name": name, "gameIDs": [],
                                "total": 0, "players": set(),
                                "icon": ability_lookup.get(gid, {}).get(
                                    "icon", ""),
                                "type": ability_lookup.get(gid, {}).get(
                                    "type", ""),
                            }
                        merged_dt[name]["players"].add(player_name)
                        merged_dt[name]["total"] += total
                        if gid and gid not in merged_dt[name]["gameIDs"]:
                            merged_dt[name]["gameIDs"].append(gid)

                # ── Parse dd: DamageDone viewBy:Target → find adds ──
                dd_entries = ((report.get("dd") or {}).get("data", {})
                              .get("entries", []))
                for e in dd_entries:
                    target_name = e.get("name", "")
                    total = e.get("total", 0)
                    if not target_name or total <= 0:
                        continue
                    # Skip the main boss — everything else is a
                    # potential add/object players interacted with
                    if target_name.lower() == boss_name.lower():
                        continue
                    if target_name not in merged_adds:
                        merged_adds[target_name] = 0
                    merged_adds[target_name] += total

                # ── Parse db: debuff auras ──
                db_entries = ((report.get("db") or {}).get("data", {})
                              .get("auras", []))
                for e in db_entries:
                    name = e.get("name", "")
                    gid = int(e.get("guid", e.get("abilityIcon", 0)))
                    if not name:
                        continue
                    if name not in boss_journal:
                        skipped_db.add(name)
                        continue
                    if name not in merged_db:
                        merged_db[name] = {"name": name, "gameIDs": []}
                    if gid and gid not in merged_db[name]["gameIDs"]:
                        merged_db[name]["gameIDs"].append(gid)

            # Convert player sets to counts for serialization
            damage_taken = []
            for name, info_dt in sorted(merged_dt.items(),
                                         key=lambda x: x[1]["total"],
                                         reverse=True):
                damage_taken.append({
                    "name": info_dt["name"],
                    "gameIDs": info_dt["gameIDs"],
                    "total": info_dt["total"],
                    "players_hit": len(info_dt["players"]),
                    "icon": info_dt["icon"],
                    "type": info_dt["type"],
                })
            debuffs = list(merged_db.values())

            # All non-boss targets, sorted by damage
            add_targets = sorted(merged_adds.items(),
                                  key=lambda x: x[1], reverse=True)

            # ── Build method suggestions from actual player counts ──
            raid_size_est = max(max_raid_size, 1)

            suggestions = {}
            for name, dt_info in merged_dt.items():
                n_players = len(dt_info["players"])
                hit_ratio = n_players / raid_size_est
                if hit_ratio < 0.30:
                    suggestions[name] = "binary_fail"
                elif hit_ratio < 0.70:
                    suggestions[name] = "relative_fail"
                else:
                    suggestions[name] = "ignore"
            # Debuffs without damage entries default to binary_fail
            for db in debuffs:
                if db["name"] not in suggestions:
                    suggestions[db["name"]] = "binary_fail"

            # ── Merge spell IDs from damage + debuff sources ──
            ability_spell_ids = {}
            for name, dt_info in merged_dt.items():
                ability_spell_ids[name] = list(dt_info.get("gameIDs", []))
            for name, db_info in merged_db.items():
                existing = ability_spell_ids.get(name, [])
                for gid in db_info.get("gameIDs", []):
                    if gid and gid not in existing:
                        existing.append(gid)
                ability_spell_ids[name] = existing

            print(f"    Kept: {len(damage_taken)} damage, {len(debuffs)} debuffs")
            # Full target breakdown
            all_dd_targets = sorted(merged_adds.items(),
                                     key=lambda x: x[1], reverse=True)
            print(f"    DamageDone targets (all non-boss): {len(all_dd_targets)}")
            for aname, admg in all_dd_targets:
                in_journal = aname in boss_creatures
                print(f"      {'[J]' if in_journal else '[ ]'} {aname} "
                      f"({admg:,.0f} damage)")
            print(f"    Suggestions: {len(suggestions)} abilities "
                  f"(raid size est: {raid_size_est})")
            print(f"    Filtered out: {len(skipped_dt)} damage, "
                  f"{len(skipped_db)} debuffs (not in journal)")

            cache["bosses"][boss_name] = {
                "encounter_id": str(enc_id),
                "reports": [(c, f, d) for c, f, d in info["reports"]],
                "damage_taken": damage_taken,
                "debuffs": debuffs,
                "add_targets": [{"name": n, "total": t}
                                for n, t in add_targets],
                "suggestions": suggestions,
                "ability_spell_ids": ability_spell_ids,
                "raid_size_est": raid_size_est,
            }

        # Preserve encounter order from WCL for this raid
        raid_boss_orders[raid_name] = [e["name"] for e in encounters
                                        if e["name"] in cache["bosses"]]

        print(f"\n  {raid_name} complete: {len(boss_order_this_raid)} bosses scanned.")

    # ══════════════════════════════════════════════════════════════
    #  Save accumulated results
    # ══════════════════════════════════════════════════════════════

    cache["spell_descriptions"] = all_id_descriptions
    cache["spell_name_descriptions"] = all_name_descriptions

    total_kept = sum(
        len(b["damage_taken"]) + len(b["debuffs"])
        for b in cache["bosses"].values())
    print(f"\n  Scan complete: {total_kept} boss abilities across "
          f"{len(cache['bosses'])} bosses.")

    # ── Save probe cache ──
    print(f"\nSaving to probe_cache.json...")
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

    # ── Auto-create boss entries in mechanic_rulesets.json ──
    print(f"\nUpdating mechanic_rulesets.json with new boss entries...")
    added = 0
    suggested_total = 0
    try:
        if os.path.isfile(MECHANIC_FILE):
            mech_data = _read_json(MECHANIC_FILE)
        else:
            mech_data = {
                "scoring_weights": {
                    "mechanics": 0.40, "deaths": 0.35,
                    "parse_performance": 0.20, "consumables": 0.05
                },
                "grade_thresholds": {"A": 90, "B": 80, "C": 70, "D": 60, "F": 0},
                "scoring_methods": {},
                "immune_classes": [],
                "ignore_abilities": [],
            }

        if "bosses" not in mech_data:
            mech_data["bosses"] = {}
        if "raids" not in mech_data:
            mech_data["raids"] = {}

        # Update raids section for each processed raid
        for rn, boss_list in raid_boss_orders.items():
            mech_data["raids"][rn] = boss_list
            print(f"  Raid \"{rn}\": {len(boss_list)} bosses (order updated)")

        skipped = 0
        for boss_name, boss_info in cache["bosses"].items():
            suggestions = boss_info.get("suggestions", {})
            spell_ids = boss_info.get("ability_spell_ids", {})
            add_targets = boss_info.get("add_targets", [])

            if boss_name in mech_data["bosses"]:
                skipped += 1
                mech_data["bosses"][boss_name]["encounter_id"] = str(
                    boss_info["encounter_id"])
                if "short_name" not in mech_data["bosses"][boss_name]:
                    mech_data["bosses"][boss_name]["short_name"] = (
                        _auto_short_name(boss_name))
                # Add suggested methods for NEW abilities only
                existing_mechs = mech_data["bosses"][boss_name].get(
                    "mechanics", {})
                existing_ignored = mech_data["bosses"][boss_name].get(
                    "ignored", [])
                new_suggested = 0
                new_ignored = 0
                moved_to_ignored = 0
                for ability, method in suggestions.items():
                    if method == "ignore":
                        # If it's in mechanics from a previous auto-suggest,
                        # move it to ignored (respect manual user edits)
                        if ability in existing_mechs:
                            if existing_mechs[ability].get("auto_suggested"):
                                del existing_mechs[ability]
                                if ability not in existing_ignored:
                                    existing_ignored.append(ability)
                                    moved_to_ignored += 1
                        elif ability not in existing_ignored:
                            existing_ignored.append(ability)
                            new_ignored += 1
                    elif ability not in existing_mechs:
                        if ability not in existing_ignored:
                            existing_mechs[ability] = {
                                "method": method,
                                "display": "",
                                "fix": "",
                                "auto_suggested": True,
                            }
                            new_suggested += 1
                # Always update spell_ids — merge new IDs with existing
                for ability in existing_mechs:
                    new_ids = spell_ids.get(ability, [])
                    old_ids = existing_mechs[ability].get("spell_ids", [])
                    merged = list(old_ids)
                    for gid in new_ids:
                        if gid and gid not in merged:
                            merged.append(gid)
                    if merged:
                        existing_mechs[ability]["spell_ids"] = merged
                mech_data["bosses"][boss_name]["mechanics"] = existing_mechs
                mech_data["bosses"][boss_name]["ignored"] = existing_ignored
                # Add NEW add targets to target_swap
                existing_ts = mech_data["bosses"][boss_name].get(
                    "target_swap", {})
                new_adds = 0
                for add in add_targets:
                    aname = add["name"]
                    if aname not in existing_ts:
                        existing_ts[aname] = {
                            "display": "", "fix": "",
                            "auto_suggested": True,
                        }
                        new_adds += 1
                mech_data["bosses"][boss_name]["target_swap"] = existing_ts
                parts = []
                if new_suggested:
                    parts.append(f"{new_suggested} new mechanics")
                if new_ignored:
                    parts.append(f"{new_ignored} new ignored")
                if moved_to_ignored:
                    parts.append(f"{moved_to_ignored} moved to ignored")
                if new_adds:
                    parts.append(f"{new_adds} new adds")
                if parts:
                    print(f"  {boss_name}: exists — {', '.join(parts)} added")
                    suggested_total += new_suggested
                else:
                    print(f"  {boss_name}: already exists -- skipped")
            else:
                # New boss: split into mechanics vs ignored
                mechs = {}
                ignored = []
                for ability, method in suggestions.items():
                    if method == "ignore":
                        ignored.append(ability)
                    else:
                        entry = {
                            "method": method,
                            "display": "",
                            "fix": "",
                            "auto_suggested": True,
                        }
                        ids = spell_ids.get(ability, [])
                        if ids:
                            entry["spell_ids"] = ids
                        mechs[ability] = entry
                # Populate target_swap from discovered adds
                ts = {}
                for add in add_targets:
                    ts[add["name"]] = {
                        "display": "", "fix": "",
                        "auto_suggested": True,
                    }
                mech_data["bosses"][boss_name] = {
                    "encounter_id": str(boss_info["encounter_id"]),
                    "short_name": _auto_short_name(boss_name),
                    "mechanics": mechs,
                    "tank_swap_rules": [],
                    "target_swap": ts,
                    "bonus_mechanics": {},
                    "ignored": ignored
                }
                added += 1
                suggested_total += len(mechs)
                print(f"  {boss_name}: created with {len(mechs)} mechanics, "
                      f"{len(ignored)} ignored, {len(ts)} adds")

        with open(MECHANIC_FILE, "w", encoding="utf-8") as f:
            json.dump(mech_data, f, indent=2, ensure_ascii=False)
        if added > 0:
            print(f"  Wrote {added} new boss(es) to mechanic_rulesets.json.")
        else:
            print(f"  No new bosses to add ({skipped} already configured).")

    except Exception as e:
        print(f"  WARNING: Could not update mechanic_rulesets.json: {e}")
        print(f"  (probe_cache.json was saved successfully)")

    elapsed = time.time() - probe_start
    print(f"\n{'=' * 60}")
    print(f"  PROBE COMPLETE")
    print(f"  WCL queries used: {query_count}")
    print(f"  Raids processed: {len(raid_boss_orders)}")
    print(f"  Bosses scanned: {len(cache['bosses'])}")
    print(f"  New bosses added to mechanics: {added}")
    print(f"  Auto-suggested mechanics: {suggested_total}")
    print(f"  Journal abilities: {len(all_journal_names)}")
    print(f"  Descriptions: {len(all_id_descriptions)} by ID, "
          f"{len(all_name_descriptions)} by name")
    print(f"  Time elapsed: {elapsed:.0f}s")
    print(f"{'=' * 60}")
    print("\nYou can now close this window.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
