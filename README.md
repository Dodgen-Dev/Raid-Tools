# Raid-Tools
================================================================================
 # RAID TOOL PROJECT — README
 # Author: Dodgen
 # Last Updated: 2026-02-24
================================================================================

  This is a free, open-source World of Warcraft raid analysis tool.  I built
  it for my own raid team and I'm offering it — code and all — to anyone who
  wants to use it.  All I ask is two things:

    1.  Credit me.  If you share the tool, the output, or build on it,
        keep attribution visible.
    2.  Help me make it better.  Bug reports, feature ideas, pull requests —
        anything is welcome.  This is a community project now.


================================================================================
# WHAT IT DOES
================================================================================

  The Raid Tool pulls data from the Warcraft Logs (WCL) v2 GraphQL API and
  the Blizzard API, then processes it into a formatted spreadsheet report
  covering every aspect of your raid team's performance.

  Output is formatted for Google Sheets.  Upload the generated .xlsx to
  Google Drive and open it in Sheets for the best experience with dropdowns,
  conditional formatting, and XLOOKUP formulas.  Excel also works, but some
  formatting may render differently.


================================================================================
 # FILES IN THIS PROJECT
================================================================================

  launcher.pyw          The desktop GUI.  This is the only file you need to
                        run directly.  It provides a tabbed interface for
                        configuration, roster management, mechanic rules,
                        and script execution.  The launcher will create all
                        needed support files (config.json, roster.json,
                        mechanic_rulesets.json, probe_cache.json) as you
                        work — you do not need to create them by hand.

  raid_pull.py          The data-gathering engine.  Authenticates with WCL
                        and Blizzard, discovers reports in your date range,
                        and pulls fight metadata, DPS/HPS rankings, deaths,
                        damage taken, consumable usage, mechanic events,
                        character profiles, gear, enchants, and guild zone
                        rankings.  Outputs raid_dataframes.xlsx containing
                        all raw data as pandas DataFrames.  Supports
                        checkpoint/resume — if it crashes mid-run, restart
                        it and it picks up where it left off.  Delete
                        raid_pull_resume.pkl to force a fresh run.

  wcl_probe.py          The mechanic discovery tool.  Scans top-ranked WCL
                        reports and cross-references the Blizzard Dungeon
                        Journal to catalogue every boss ability in the
                        current raid zone.  Populates the Mechanics tab in
                        the launcher with suggested rulesets (binary_fail,
                        relative_fail, ignore, etc.) so you can fine-tune
                        which abilities are tracked and scored.

  build_tracker_v4.py   The report builder.  Reads raid_dataframes.xlsx,
                        roster.json, and mechanic_rulesets.json, then scores
                        every player on mechanics, deaths, parses, and
                        consumables.  Generates the final formatted
                        spreadsheet with these sheets:

                          Summary          — Raid readiness, gear status,
                                             enchants, sockets, tier count.
                          Raids            — Best Score and Average Score
                                             vs raid average, per-boss.
                          Raid Performance — Night-by-night breakdown with
                                             date dropdown, boss results,
                                             and per-player rankings.
                          Character View   — Deep dive per character with
                                             parse history, death analysis,
                                             mechanic fails, and trends.
                          Roster           — Full organized roster, class
                                             composition, and buff tracker.

                        Several hidden data sheets power the dropdowns and
                        charts.  These are auto-generated — do not edit them.


================================================================================
 # GENERATED FILES (created automatically by the launcher and scripts)
================================================================================

  config.json             API credentials, guild/team identity, raid zone
                          names, date ranges, consumable spell IDs, output
                          directory, and score weights.  Created and edited
                          through the launcher's Config and Score Weights tabs.

  roster.json             Player roster with mains, alts, roles, exclusions.
                          Created and edited through the launcher's Roster tab.
                          Auto-updated by raid_pull.py when new characters are
                          discovered in logs.

  mechanic_rulesets.json  Per-boss ability tracking rules.  Created by the
                          WCL Probe and edited through the launcher's
                          Mechanics tab.

  probe_cache.json        Cached results from the most recent WCL Probe run.
                          Used by the Mechanics tab to avoid re-probing.

  raid_dataframes.xlsx    Raw pulled data (pandas DataFrames + type schema).
                          The intermediate file between raid_pull and
                          build_tracker.  Not intended for direct reading.

  raid_pull_resume.pkl    Checkpoint file for resuming interrupted pulls.
                          Delete this to force a completely fresh pull.

  debug.log               Launcher debug output.

  launcher_error.log      Written only if the launcher crashes on startup.


================================================================================
#  REQUIREMENTS
================================================================================

  Python 3.10 or newer.

  Required packages:
    pip install requests pandas openpyxl numpy

  Standard library modules used (no install needed):
    tkinter, json, os, sys, re, time, pickle, subprocess, threading,
    webbrowser, collections, itertools, datetime, zipfile, shutil, tempfile

  API credentials (free):
    1.  Warcraft Logs — Create a client at https://www.warcraftlogs.com/profile
        (scroll to "Web API" → "Create Client").
    2.  Blizzard — Register at https://develop.battle.net and create an API
        client for character profile and gear data.

  Note on the Blizzard API and Patch 12.0.0:
    Be aware that Blizzard has made significant API changes in Patch 12.0.0.
    A full list of removed, renamed, and restricted API endpoints is
    documented at:  https://warcraft.wiki.gg/wiki/Patch_12.0.0/API_changes
    This tool already accounts for the current API surface, but if you are
    modifying or extending the code, consult that page to ensure you are not
    calling removed or deprecated endpoints.


================================================================================
 # FIRST-TIME SETUP
================================================================================

  1.  Place all four script files in the same folder:
        launcher.pyw, raid_pull.py, wcl_probe.py, build_tracker_v4.py

  2.  Run launcher.pyw.
        On Windows: double-click it (the .pyw extension runs without a
        console window).  Alternatively: python launcher.pyw

  3.  Go to the Config tab:
        a.  Enter your WCL Client ID and Client Secret.
        b.  Enter your Blizzard Client ID and Client Secret.
        c.  Fill in your Guild Name, Team Name, Server (realm slug),
            Region (US or EU), and Guild IDs.
            (Find Guild IDs in your WCL guild page URL:
             warcraftlogs.com/guild/id/XXXXXX)
        d.  Add your raid zone name(s) and check the box to enable them.
        e.  Set your Start Date (YYYY-MM-DD).
        f.  Set an Output Directory (Google Drive path recommended).
        g.  Click "Save Config".

  4.  Run the WCL Probe (button at the bottom of the Config tab):
        This discovers all bosses and abilities in your enabled raid zone
        and populates the Mechanics tab with suggested tracking rules.

  5.  Review the Mechanics tab:
        Adjust ability rules if needed (binary_fail, relative_fail, ignore).
        Save when done.

  6.  Go to the Run Scripts tab and click "Run Both (Chained)":
        This runs raid_pull.py first, then build_tracker_v4.py automatically.
        A popup console shows live progress.

  7.  Open the output .xlsx from your Output Directory in Google Sheets.


================================================================================
 # WEEKLY WORKFLOW
================================================================================

  Once setup is complete, the weekly routine is:

    1.  Open the launcher.
    2.  (Optional) Confirm your roster is locked on the Roster tab.
    3.  Click "Run Both (Chained)" on the Run Scripts tab.
    4.  Open the output spreadsheet in Google Sheets.

  That's it.  Everything else is one-time setup.

  If you've only made roster changes (reassigned players, excluded someone),
  use "Rebuild Report" on the Roster tab instead of re-pulling.  It reuses
  the existing data and regenerates the spreadsheet instantly.

	You can also use the schedule option in the run scripts tab to automatically
	set up a task in Windows Task Scheduler so the program will run in the 
	background as long as your computer is on.

================================================================================
 # DATA PIPELINE OVERVIEW
================================================================================

  Stage 1 — Raid Pull (raid_pull.py):

    Phase 1:  Discover report codes from WCL for your guild and date range.
    Phase 2:  Pull fight metadata (boss names, kill/wipe, players per fight).
    Phase 3:  Pull DPS/HPS rankings for every fight.
    Phase 4:  Pull deaths, damage taken, abilities, consumable usage.
    Phase 4.1: Pull mechanic-specific data (interrupts, target swaps, debuffs).
    Phase 4.5: Pull DPS/HPS for wipe fights (separate from ranked kills).
    Phase 5:  Build unique player list from filtered fights.
    Phase 6:  Blizzard API — character profiles, gear, enchants, progression.
    Phase 8:  Guild zone rankings from WCL.
    Phase 9:  Update roster.json with newly discovered characters.
    Output:   raid_dataframes.xlsx

  Stage 2 — Build Tracker (build_tracker_v4.py):

    Reads raid_dataframes.xlsx + roster.json + mechanic_rulesets.json.
    Scores every player on mechanics, deaths, parses, and consumables.
    Generates the final formatted report spreadsheet.


================================================================================
 # TROUBLESHOOTING
================================================================================

  No reports found:
    - Check that Start Date is not in the future.
    - Verify your WCL Guild IDs are correct (check the URL on WCL).
    - Try enabling "Include parent guild logs" if your team ID is new.
    - Make sure the raid zone is enabled and spelled correctly in Config.

  Too many irrelevant fights from other teams:
    - Lock your roster.  This activates the min_roster_players filter (8).
    - Only fights with 8+ of your roster members will be processed.

  Characters keep getting re-added after exclusion:
    - Make sure you save the roster after excluding characters.
    - Excluded characters are stored in roster.json and checked during pulls.

  Blizzard API returning iLvl:ERR or 0 slots:
    - The character may have a special character in their name that does not
      URL-encode cleanly, or they may be on a different region.
    - This is cosmetic — scoring still works from WCL data alone.

  Build Tracker crashes with TypeError:
    - Usually caused by unexpected data types (NaN values in class/spec).
    - Re-pull data to refresh.  If persistent, check debug.log.

  Script seems stuck or frozen:
    - Check the popup console — it shows live progress.
    - Large date ranges with many reports take time (especially Phase 6).
    - Blizzard API has rate limits — the script auto-throttles.
    - WCL point usage is logged.  Check if you are near the hourly limit.


================================================================================
#  TIPS
================================================================================

  - New raid tier:  Update the raid zone name in Config, run the Probe to
    detect new boss mechanics, review the Mechanics tab, then pull.

  - Mid-season roster changes:  New players are auto-discovered on pull.
    Assign them in the Roster tab and rebuild.

  - Mechanic tuning:  Start with the Probe's suggestions, then review the
    first report.  If an ability shows too many false fails (unavoidable
    damage flagged as binary_fail), switch it to relative_fail or ignore.

  - Performance:  A full-season pull (3+ months, 20+ reports) takes 5-15
    minutes depending on fight count.  Subsequent pulls with checkpointing
    are faster.

  - Scheduling:  The launcher supports both a repeat timer (runs while the
    launcher is open) and Windows Task Scheduler integration (runs even
    when the launcher is closed).  Set it for the morning after raid night.


================================================================================
#  LICENSE & ATTRIBUTION
================================================================================

  This tool is free to use, modify, and share.  If you use it, credit the
  original author.  If you improve it, share your changes so everyone
  benefits.  If you have any questions leave me a message.  I sometimes check
  dodgendan@gmail.com, or I can generally be found at https://discord.gg/q8y8pwPZaN
  where I will most quickly react to bug reports.  

  This project uses:
    - Warcraft Logs v2 GraphQL API (https://www.warcraftlogs.com)
    - Blizzard API (https://develop.battle.net)
================================================================================
