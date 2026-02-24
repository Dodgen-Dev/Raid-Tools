"""
Raid Tools Launcher — Desktop application for managing and running
raid_pull.py and build_tracker_v4.py with configuration editing.

Place in the same folder as raid_pull.py, build_tracker_v4.py,
config.json, roster.json, and mechanic_rulesets.json.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog
import subprocess
import threading
import json
import os
import sys
import webbrowser
from datetime import datetime

# ── Paths — everything lives next to this launcher ──
IS_FROZEN = getattr(sys, "frozen", False)
if IS_FROZEN:
    SCRIPT_DIR = os.path.dirname(sys.executable)
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

if IS_FROZEN:
    RAID_PULL = os.path.join(SCRIPT_DIR, "raid_pull.exe")
    BUILD_TRACKER = os.path.join(SCRIPT_DIR, "build_tracker_v4.exe")
    WCL_PROBE = os.path.join(SCRIPT_DIR, "wcl_probe.exe")
else:
    RAID_PULL = os.path.join(SCRIPT_DIR, "raid_pull.py")
    BUILD_TRACKER = os.path.join(SCRIPT_DIR, "build_tracker_v4.py")
    WCL_PROBE = os.path.join(SCRIPT_DIR, "wcl_probe.py")
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")
ROSTER_FILE = os.path.join(SCRIPT_DIR, "roster.json")
MECHANIC_FILE = os.path.join(SCRIPT_DIR, "mechanic_rulesets.json")
PROBE_CACHE = os.path.join(SCRIPT_DIR, "probe_cache.json")

TASK_NAME = "RaidToolsLauncher"
DEBUG_LOG = os.path.join(SCRIPT_DIR, "debug.log")

# ── Globals ──
current_process = None
process_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════
#  Debug log
# ═══════════════════════════════════════════════════════════════

def debug_log(text):
    """Append a line to the debug log file."""
    try:
        with open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
#  File I/O
# ═══════════════════════════════════════════════════════════════

def load_json(path, default=None):
    if default is None:
        default = {}
    if not os.path.isfile(path):
        return default
    try:
        raw = open(path, "rb").read()
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")
        # Re-save as clean UTF-8 so it doesn't happen again
        try:
            with open(path, "w", encoding="utf-8") as fix:
                fix.write(text)
        except OSError:
            pass
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        messagebox.showwarning(
            "JSON Error",
            f"Could not parse {os.path.basename(path)}:\n\n{e}\n\n"
            f"Starting with defaults. Fix the file or re-save from this app."
        )
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════

def _auto_short_name(name):
    """Generate a short display name from a boss name."""
    if "," in name:
        name = name.split(",")[0].strip()
    if name.startswith("The "):
        return name[4:]
    words = name.split()
    if len(words) <= 1:
        return name
    first = words[0]
    if "-" in first:
        return first
    return first


# ═══════════════════════════════════════════════════════════════
#  Tab: Config
# ═══════════════════════════════════════════════════════════════

class ConfigTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.entries = {}
        self.show_vars = {}
        self._build_ui()
        self._load()

    def _build_ui(self):
        # ── Scrollable container ──
        container = ttk.Frame(self)
        container.pack(fill=tk.BOTH, expand=True)

        self.canvas = tk.Canvas(container)
        v_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL,
                                  command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=v_scroll.set)
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.inner = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0),
                                                        window=self.inner,
                                                        anchor=tk.NW)
        self.inner.bind("<Configure>",
                        lambda e: self.canvas.configure(
                            scrollregion=self.canvas.bbox("all")))
        self.canvas.bind("<Configure>",
                         lambda e: self.canvas.itemconfig(
                             self.canvas_window,
                             width=max(e.width,
                                       self.inner.winfo_reqwidth())))
        # ── Blizzard section ──
        bliz_frame = ttk.LabelFrame(self.inner, text="Blizzard API")
        bliz_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        self._add_field(bliz_frame, "Client ID:", "bliz_client_id", row=0)
        self._add_field(bliz_frame, "Client Secret:", "bliz_client_secret",
                        row=1, secret=True)
        self._add_field(bliz_frame, "Region:", "bliz_region", row=2, width=10)

        btn_bliz = ttk.Button(
            bliz_frame, text="Get API Key \u2197",
            command=lambda: webbrowser.open(
                "https://community.developer.battle.net/documentation/guides/getting-started"))
        btn_bliz.grid(row=3, column=0, columnspan=2, sticky=tk.W,
                      padx=10, pady=(2, 8))
        Tooltip(btn_bliz, lambda: (
            "Opens the Battle.net Developer Portal getting-started guide.\n"
            "You need a Blizzard API client ID and secret to pull\n"
            "character gear and guild data."))

        # ── WCL section ──
        wcl_frame = ttk.LabelFrame(self.inner, text="Warcraft Logs API")
        wcl_frame.pack(fill=tk.X, padx=10, pady=5)

        self._add_field(wcl_frame, "Client ID:", "wcl_client_id", row=0)
        self._add_field(wcl_frame, "Client Secret:", "wcl_client_secret",
                        row=1, secret=True)

        btn_wcl = ttk.Button(
            wcl_frame, text="Get API Key \u2197",
            command=lambda: webbrowser.open(
                "https://www.warcraftlogs.com/profile"))
        btn_wcl.grid(row=2, column=0, columnspan=2, sticky=tk.W,
                     padx=10, pady=(2, 8))
        Tooltip(btn_wcl, lambda: (
            "Opens your Warcraft Logs profile page.\n"
            "Scroll down to the 'Web API' section and click\n"
            "'Create Client' to generate a Client ID and Secret."))

        # ── Guild / Team Identity ──
        guild_frame = ttk.LabelFrame(self.inner, text="Guild / Team Identity")
        guild_frame.pack(fill=tk.X, padx=10, pady=5)

        self._add_field(guild_frame, "Guild Name:", "guild_name",
                        row=0, width=30)
        self._add_field(guild_frame, "Team Name:", "team_name",
                        row=1, width=30)
        self._add_field(guild_frame, "Server:", "server",
                        row=2, width=25)
        self._add_field(guild_frame, "Region:", "region",
                        row=3, width=10)
        self._add_field(guild_frame, "Team Guild ID:", "team_guild_id",
                        row=4, width=15)
        self._add_field(guild_frame, "Parent Guild ID:", "parent_guild_id",
                        row=5, width=15)

        tk.Label(guild_frame,
                 text="Guild Name is your WoW guild. Team Name is your raid "
                      "team within that guild (used for the output filename). "
                      "Server is the realm slug (e.g. stormrage). Region is "
                      "US or EU. Find Guild IDs on your WCL guild page URL: "
                      "warcraftlogs.com/guild/id/XXXXXX. If you only have one "
                      "guild with no teams, put the same ID in both.",
                 font=("Segoe UI", 11), fg="#555555",
                 wraplength=700, justify=tk.LEFT
                 ).grid(row=6, column=0, columnspan=3, sticky=tk.W,
                        padx=10, pady=(0, 5))

        # ── Raid / Dates section ──
        raid_frame = ttk.LabelFrame(self.inner, text="Raid Settings")
        raid_frame.pack(fill=tk.X, padx=10, pady=5)

        # Raid name rows with enable checkboxes
        self.raid_rows = []
        MAX_RAID_ROWS = 4
        ttk.Label(raid_frame, text="Raids:").grid(
            row=0, column=0, sticky=tk.W, padx=(10, 5), pady=3)
        for i in range(MAX_RAID_ROWS):
            var = tk.BooleanVar(value=False)
            cb = ttk.Checkbutton(raid_frame, variable=var)
            cb.grid(row=i, column=1, padx=(5, 0), pady=2)
            entry = ttk.Entry(raid_frame, width=35)
            entry.grid(row=i, column=2, sticky=tk.W, padx=5, pady=2)
            self.raid_rows.append({"enabled": var, "entry": entry})

        date_row = MAX_RAID_ROWS
        self._add_field(raid_frame, "Start Date (YYYY-MM-DD):", "start_date",
                        row=date_row, width=15)
        self._add_field(raid_frame, "End Date (YYYY-MM-DD):", "end_date",
                        row=date_row + 1, width=15)
        self._add_field(raid_frame, "Patch Date (YYYY-MM-DD):", "patch_date",
                        row=date_row + 2, width=15)

        tk.Label(raid_frame,
                 text="Dates must be YYYY-MM-DD (e.g. 2026-01-20). "
                      "Start Date controls which reports are pulled. "
                      "End Date is optional — leave blank to pull through today. "
                      "Patch Date is used as a fallback if Start Date is empty.",
                 font=("Segoe UI", 11), fg="#555555", wraplength=700, justify=tk.LEFT
                 ).grid(row=date_row + 3, column=0, columnspan=3, sticky=tk.W,
                        padx=10, pady=(0, 5))

        # ── Partition overrides ──
        part_frame = ttk.LabelFrame(self.inner, text="Partition Override (blank = auto-detect)")
        part_frame.pack(fill=tk.X, padx=10, pady=5)

        self._add_field(part_frame, "Raid Partition:", "raid_partition",
                        row=0, width=10)

        # ── Output ──
        out_frame = ttk.LabelFrame(self.inner, text="Output")
        out_frame.pack(fill=tk.X, padx=10, pady=5)

        self._add_field(out_frame, "Output Directory:", "output_dir",
                        row=0, width=55)
        ttk.Button(out_frame, text="Browse…",
                   command=self._browse_output_dir).grid(
                       row=0, column=3, padx=(5, 10), pady=2)
        tk.Label(out_frame,
                 text="Google Drive path recommended — sheets are formatted "
                      "for Google Sheets.",
                 font=("Segoe UI", 10), fg="#555555"
                 ).grid(row=1, column=0, columnspan=3, sticky=tk.W,
                        padx=10, pady=(0, 5))

        # ── Consumable Spell IDs ──
        con_frame = ttk.LabelFrame(
            self.inner, text="Consumable Spell IDs (comma-separated)")
        con_frame.pack(fill=tk.X, padx=10, pady=5)

        self.con_entries = {}
        con_labels = [
            ("tempered_potion", "DPS Potions:"),
            ("healing_potion", "Healing Potions:"),
            ("healthstone", "Healthstones:"),
        ]
        for i, (key, label) in enumerate(con_labels):
            ttk.Label(con_frame, text=label).grid(
                row=i, column=0, padx=(10, 5), pady=3, sticky=tk.W)
            e = ttk.Entry(con_frame, width=45)
            e.grid(row=i, column=1, padx=5, pady=3, sticky=tk.W)
            self.con_entries[key] = e

        btn_wcl_con = ttk.Button(
            con_frame, text="Find Spell IDs \u2197",
            command=lambda: webbrowser.open(
                "https://www.warcraftlogs.com/profile"))
        btn_wcl_con.grid(row=len(con_labels), column=0, columnspan=2,
                         sticky=tk.W, padx=10, pady=(2, 8))
        Tooltip(btn_wcl_con, lambda: (
            "Opens Warcraft Logs. To find a spell ID:\n"
            "search for an item or spell on WCL, then look at\n"
            "the URL — the number at the end is the spell ID.\n"
            "e.g. warcraftlogs.com/reports/…#ability=431932\n"
            "→ spell ID is 431932"))

        # ── Save button (outside scroll area so always visible) ──
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="Save Config",
                   command=self._save).pack(side=tk.LEFT)
        self.save_label = ttk.Label(btn_frame, text="")
        self.save_label.pack(side=tk.LEFT, padx=10)

        # ── WCL Probe (step 2 after saving config) ──
        probe_frame = ttk.LabelFrame(self, text="Step 2 — Probe WCL")
        probe_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        self.btn_probe = ttk.Button(probe_frame, text="Run WCL Probe",
                                     command=self._run_probe_via_mechanics)
        self.btn_probe.pack(side=tk.LEFT, padx=10, pady=8)

        self.probe_status = ttk.Label(probe_frame, text="")
        self.probe_status.pack(side=tk.LEFT, padx=10)

        # Show cached probe status if available
        _pcache = load_json(PROBE_CACHE, {})
        if _pcache.get("probe_date"):
            _zn = _pcache.get("zone", {}).get("name", "")
            _nb = len(_pcache.get("bosses", {}))
            self.probe_status.configure(
                text=f"Cache: {_zn} ({_nb} bosses) — {_pcache['probe_date']}",
                foreground="green")

        tk.Label(probe_frame,
                 text="Save your config first, then run the probe to discover "
                      "bosses and abilities. This populates the Mechanics tab.",
                 font=("Segoe UI", 11), fg="#555555",
                 wraplength=600, justify=tk.LEFT
                 ).pack(side=tk.LEFT, padx=10, pady=8)

    def _run_probe_via_mechanics(self):
        """Delegate probe execution to the MechanicsTab which owns the logic."""
        if hasattr(self, "_mechanics_tab_ref") and self._mechanics_tab_ref:
            self._mechanics_tab_ref._run_probe()
        else:
            from tkinter import messagebox
            messagebox.showwarning("Probe",
                                   "Cannot find Mechanics tab reference.")

    def _add_field(self, parent, label, key, row, width=35, secret=False):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W,
                                           padx=(10, 5), pady=3)
        if secret:
            entry = ttk.Entry(parent, width=width, show="*")
            entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

            show_var = tk.BooleanVar(value=False)
            self.show_vars[key] = show_var

            def toggle(e=entry, v=show_var):
                e.configure(show="" if v.get() else "*")

            ttk.Checkbutton(parent, text="Show", variable=show_var,
                            command=toggle).grid(row=row, column=2, padx=5)
        else:
            entry = ttk.Entry(parent, width=width)
            entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        self.entries[key] = entry

    def _browse_output_dir(self):
        from tkinter import filedialog
        folder = filedialog.askdirectory(
            title="Select Output Directory",
            initialdir=self.entries["output_dir"].get().strip() or None)
        if folder:
            self.entries["output_dir"].delete(0, tk.END)
            self.entries["output_dir"].insert(0, folder)

    def _load(self):
        cfg = load_json(CONFIG_FILE, {})
        bliz = cfg.get("blizzard", {})
        wcl = cfg.get("warcraftlogs", {})

        fields = {
            "bliz_client_id": bliz.get("client_id", ""),
            "bliz_client_secret": bliz.get("client_secret", ""),
            "bliz_region": bliz.get("region", "us"),
            "wcl_client_id": wcl.get("client_id", ""),
            "wcl_client_secret": wcl.get("client_secret", ""),
            "guild_name": cfg.get("guild_name", ""),
            "team_name": cfg.get("team_name", ""),
            "server": cfg.get("server", ""),
            "region": cfg.get("region", "US"),
            "team_guild_id": str(cfg.get("team_guild_id", "")),
            "parent_guild_id": str(cfg.get("parent_guild_id", "")),
            "patch_date": cfg.get("patch_date", ""),
            "start_date": cfg.get("start_date", ""),
            "end_date": cfg.get("end_date", ""),
            "raid_partition": str(cfg.get("raid_partition", "")),
            "output_dir": cfg.get("output_dir", ""),
        }

        for key, val in fields.items():
            self.entries[key].delete(0, tk.END)
            self.entries[key].insert(0, val)

        # Load raids list (backward compat: convert old current_raid)
        raids = cfg.get("raids", [])
        if not raids and cfg.get("current_raid"):
            raids = [{"name": cfg["current_raid"], "enabled": True}]
        for i, row in enumerate(self.raid_rows):
            if i < len(raids):
                row["entry"].delete(0, tk.END)
                row["entry"].insert(0, raids[i].get("name", ""))
                row["enabled"].set(raids[i].get("enabled", False))
            else:
                row["entry"].delete(0, tk.END)
                row["enabled"].set(False)

        # Load consumable spell IDs
        cons = cfg.get("consumables", {})
        defaults = {
            "tempered_potion": [431932, 431914, 431934, 431936],
            "healing_potion": [431416, 431418],
            "healthstone": [6262],
        }
        for key, entry in self.con_entries.items():
            ids = cons.get(key, defaults.get(key, []))
            entry.delete(0, tk.END)
            entry.insert(0, ", ".join(str(i) for i in ids))

    def _save(self):
        e = self.entries
        cfg = {
            "blizzard": {
                "client_id": e["bliz_client_id"].get().strip(),
                "client_secret": e["bliz_client_secret"].get().strip(),
                "region": e["bliz_region"].get().strip() or "us",
            },
            "warcraftlogs": {
                "client_id": e["wcl_client_id"].get().strip(),
                "client_secret": e["wcl_client_secret"].get().strip(),
            },
            "guild_name": e["guild_name"].get().strip(),
            "team_name": e["team_name"].get().strip(),
            "server": e["server"].get().strip(),
            "region": e["region"].get().strip() or "US",
            "patch_date": e["patch_date"].get().strip(),
            "start_date": e["start_date"].get().strip(),
            "end_date": e["end_date"].get().strip(),
        }

        # Raids list
        raids = []
        for row in self.raid_rows:
            name = row["entry"].get().strip()
            if name:
                raids.append({"name": name, "enabled": row["enabled"].get()})
        cfg["raids"] = raids

        # Guild IDs — only include if non-empty
        tg = e["team_guild_id"].get().strip()
        pg = e["parent_guild_id"].get().strip()
        if tg:
            cfg["team_guild_id"] = int(tg)
        if pg:
            cfg["parent_guild_id"] = int(pg)

        # Partition override — only include if non-empty
        rp = e["raid_partition"].get().strip()
        if rp:
            cfg["raid_partition"] = int(rp)

        # Output dir
        od = e["output_dir"].get().strip()
        if od:
            cfg["output_dir"] = od

        # Consumable spell IDs
        cons = {}
        for key, entry in self.con_entries.items():
            raw = entry.get().strip()
            ids = []
            for part in raw.split(","):
                part = part.strip()
                if part.isdigit():
                    ids.append(int(part))
            cons[key] = ids
        cfg["consumables"] = cons

        save_json(CONFIG_FILE, cfg)
        self.save_label.configure(text="Saved!", foreground="green")
        self.after(2000, lambda: self.save_label.configure(text=""))

        # Push team identity to Roster tab
        roster_tab = getattr(self, "_roster_tab_ref", None)
        if roster_tab and hasattr(roster_tab, "team_entries"):
            push = {
                "guild_name": e["guild_name"].get().strip(),
                "team_name": e["team_name"].get().strip(),
                "server": e["server"].get().strip(),
                "region": e["region"].get().strip() or "US",
            }
            for key, val in push.items():
                if key in roster_tab.team_entries:
                    roster_tab.team_entries[key].delete(0, tk.END)
                    roster_tab.team_entries[key].insert(0, val)


# ═══════════════════════════════════════════════════════════════
#  Tab: Run Scripts
# ═══════════════════════════════════════════════════════════════

class RunTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.timer_id = None
        self._build_ui()
        self._check_task_status()

    def _build_ui(self):
        # ── Immediate run ──
        run_frame = ttk.LabelFrame(self, text="Run Now")
        run_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        self.btn_raid = ttk.Button(run_frame, text="Run Raid Pull",
                                   command=self.run_raid_pull)
        self.btn_raid.pack(side=tk.LEFT, padx=5, pady=5)

        self.btn_build = ttk.Button(run_frame, text="Run Build Tracker",
                                    command=self.run_build_tracker)
        self.btn_build.pack(side=tk.LEFT, padx=5, pady=5)

        self.btn_both = ttk.Button(run_frame, text="Run Both (Chained)",
                                   command=self.run_both)
        self.btn_both.pack(side=tk.LEFT, padx=5, pady=5)

        self.btn_stop = ttk.Button(run_frame, text="Stop",
                                   command=self.stop_process, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.RIGHT, padx=5, pady=5)

        # ── Built-in timer ──
        timer_frame = ttk.LabelFrame(self, text="Repeat Timer (while app is open)")
        timer_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(timer_frame, text="Run every").pack(side=tk.LEFT, padx=(10, 5),
                                                       pady=5)
        self.timer_hours = tk.Spinbox(timer_frame, from_=1, to=168, width=4)
        self.timer_hours.pack(side=tk.LEFT, pady=5)
        self.timer_hours.delete(0, tk.END)
        self.timer_hours.insert(0, "12")
        ttk.Label(timer_frame, text="hours:").pack(side=tk.LEFT, padx=(2, 10),
                                                    pady=5)

        self.timer_script = ttk.Combobox(timer_frame, state="readonly", width=18,
                                          values=["Raid Pull", "Build Tracker",
                                                  "Both (Chained)"])
        self.timer_script.set("Both (Chained)")
        self.timer_script.pack(side=tk.LEFT, padx=5, pady=5)

        self.btn_timer_start = ttk.Button(timer_frame, text="Start Timer",
                                           command=self._start_timer)
        self.btn_timer_start.pack(side=tk.LEFT, padx=5, pady=5)

        self.btn_timer_stop = ttk.Button(timer_frame, text="Stop Timer",
                                          command=self._stop_timer,
                                          state=tk.DISABLED)
        self.btn_timer_stop.pack(side=tk.LEFT, padx=5, pady=5)

        self.timer_status = ttk.Label(timer_frame, text="Timer: Off")
        self.timer_status.pack(side=tk.LEFT, padx=10, pady=5)

        # ── Windows Task Scheduler ──
        sched_frame = ttk.LabelFrame(self,
                                      text="Windows Task Scheduler (runs even when app is closed)")
        sched_frame.pack(fill=tk.X, padx=10, pady=5)

        row1 = ttk.Frame(sched_frame)
        row1.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(row1, text="Run:").pack(side=tk.LEFT, padx=(0, 5))
        self.sched_script = ttk.Combobox(row1, state="readonly", width=18,
                                          values=["Raid Pull", "Build Tracker",
                                                  "Both (Chained)"])
        self.sched_script.set("Both (Chained)")
        self.sched_script.pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="Every:").pack(side=tk.LEFT, padx=(15, 5))
        self.sched_day = ttk.Combobox(row1, state="readonly", width=12,
                                       values=["Monday", "Tuesday", "Wednesday",
                                               "Thursday", "Friday", "Saturday",
                                               "Sunday"])
        self.sched_day.set("Tuesday")
        self.sched_day.pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="at:").pack(side=tk.LEFT, padx=(10, 5))
        self.sched_hour = tk.Spinbox(row1, from_=0, to=23, width=3,
                                      format="%02.0f")
        self.sched_hour.pack(side=tk.LEFT)
        self.sched_hour.delete(0, tk.END)
        self.sched_hour.insert(0, "06")
        ttk.Label(row1, text=":").pack(side=tk.LEFT)
        self.sched_min = tk.Spinbox(row1, from_=0, to=59, width=3,
                                     format="%02.0f")
        self.sched_min.pack(side=tk.LEFT)
        self.sched_min.delete(0, tk.END)
        self.sched_min.insert(0, "00")

        row2 = ttk.Frame(sched_frame)
        row2.pack(fill=tk.X, padx=10, pady=(0, 5))

        ttk.Button(row2, text="Register Schedule",
                   command=self._register_task).pack(side=tk.LEFT, padx=5)
        ttk.Button(row2, text="Remove Schedule",
                   command=self._remove_task).pack(side=tk.LEFT, padx=5)
        self.sched_status = ttk.Label(row2, text="")
        self.sched_status.pack(side=tk.LEFT, padx=10)

        # ── Pull Options ──
        opts_frame = ttk.LabelFrame(self, text="Pull Options")
        opts_frame.pack(fill=tk.X, padx=10, pady=5)

        cfg = load_json(CONFIG_FILE, {})
        self.parent_guild_var = tk.BooleanVar(
            value=cfg.get("include_parent_guild", False))
        ttk.Checkbutton(opts_frame,
                        text="Include parent guild logs (use when team ID was created mid-season)",
                        variable=self.parent_guild_var,
                        command=self._save_pull_options
                        ).pack(side=tk.LEFT, padx=10, pady=5)

        # ── Console output ──
        self.output = scrolledtext.ScrolledText(self, wrap=tk.WORD,
                                                 state=tk.DISABLED,
                                                 font=("Consolas", 11))
        self.output.pack(pady=(5, 5), padx=10, fill=tk.BOTH, expand=True)

        # ── Status bar ──
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN,
                  anchor=tk.W).pack(fill=tk.X, side=tk.BOTTOM)

    # ── Console helpers ──

    def _save_pull_options(self):
        """Write pull option toggles to config.json."""
        cfg = load_json(CONFIG_FILE, {})
        cfg["include_parent_guild"] = self.parent_guild_var.get()
        save_json(CONFIG_FILE, cfg)

    def _append(self, text):
        self.output.configure(state=tk.NORMAL)
        self.output.insert(tk.END, text)
        self.output.see(tk.END)
        self.output.configure(state=tk.DISABLED)

    def _set_running(self, running):
        state = tk.DISABLED if running else tk.NORMAL
        self.btn_raid.configure(state=state)
        self.btn_build.configure(state=state)
        self.btn_both.configure(state=state)
        self.btn_stop.configure(state=tk.NORMAL if running else tk.DISABLED)

    # ── Script runner ──

    def _run_script(self, script_path, label):
        global current_process
        if not os.path.isfile(script_path):
            self.after(0, self._append,
                       f"ERROR: {script_path} not found.\n")
            return False

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"\n{'='*50}\n  Starting: {label}  ({timestamp})\n{'='*50}\n"
        self.after(0, self._append, header)
        self.after(0, self.status_var.set, f"Running: {label}")
        debug_log(header)

        # ── Build popup console window ──
        pw = tk.Toplevel(self)
        pw.title(f"{label} — Running")
        pw.geometry("850x500")
        pw.resizable(True, True)

        warn = tk.Label(
            pw,
            text=("\u26a0  Do not close the launcher while this script is "
                  "running.  This window will update live."),
            fg="red", font=("Segoe UI", 11, "bold"),
            wraplength=1000, justify=tk.LEFT)
        warn.pack(padx=10, pady=(8, 4))

        popup_text = scrolledtext.ScrolledText(
            pw, wrap=tk.WORD, font=("Consolas", 11),
            state=tk.DISABLED, bg="#1e1e1e", fg="#cccccc",
            insertbackground="#cccccc")
        popup_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        close_btn = ttk.Button(pw, text="Close", state=tk.DISABLED,
                               command=pw.destroy)
        close_btn.pack(pady=(4, 8))

        _popup_running = [True]

        def on_popup_close():
            if _popup_running[0]:
                messagebox.showwarning(
                    "Script Running",
                    f"{label} is still running.\n\n"
                    "Please wait for it to finish before closing.",
                    parent=pw)
            else:
                pw.destroy()

        pw.protocol("WM_DELETE_WINDOW", on_popup_close)

        def popup_append(text):
            try:
                if not pw.winfo_exists():
                    return
            except tk.TclError:
                return
            popup_text.configure(state=tk.NORMAL)
            popup_text.insert(tk.END, text)
            popup_text.see(tk.END)
            popup_text.configure(state=tk.DISABLED)

        # ── Launch process ──
        kwargs = {}
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUNBUFFERED"] = "1"

        if IS_FROZEN:
            cmd = [script_path]
        else:
            cmd = [sys.executable, "-u", script_path]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            cwd=SCRIPT_DIR, bufsize=1,
            env=env, **kwargs
        )
        with process_lock:
            current_process = proc

        for line in proc.stdout:
            self.after(0, self._append, line)
            self.after(0, popup_append, line)
            debug_log(line)

        proc.wait()
        with process_lock:
            current_process = None

        _popup_running[0] = False
        success = proc.returncode == 0

        if success:
            msg = f"  {label} finished successfully.\n"
        else:
            msg = f"  {label} exited with code {proc.returncode}.\n"

        self.after(0, self._append, msg)
        self.after(0, popup_append, msg)
        debug_log(msg)

        def finalize_popup():
            try:
                if pw.winfo_exists():
                    pw.title(f"{label} — {'Complete' if success else 'Failed'}")
                    warn.configure(text="")
                    close_btn.configure(state=tk.NORMAL)
            except tk.TclError:
                pass

        self.after(0, finalize_popup)
        return success

    def _run_in_thread(self, target):
        self._set_running(True)
        threading.Thread(target=target, daemon=True).start()

    def _run_choice(self, choice):
        """Run based on a combobox selection string."""
        if choice == "Raid Pull":
            return self._run_script(RAID_PULL, "Raid Pull")
        elif choice == "Build Tracker":
            return self._run_script(BUILD_TRACKER, "Build Tracker")
        else:
            ok = self._run_script(RAID_PULL, "Raid Pull")
            if ok:
                self.after(0, self._reload_roster)
                return self._run_script(BUILD_TRACKER, "Build Tracker")
            else:
                msg = "\nRaid Pull failed — skipping Build Tracker.\n"
                self.after(0, self._append, msg)
                debug_log(msg)
                return False

    # ── Immediate run buttons ──

    def _reload_roster(self):
        """Refresh the Roster tab after a pull updates roster.json."""
        roster_tab = getattr(self, "_roster_tab_ref", None)
        if roster_tab and hasattr(roster_tab, "_load"):
            roster_tab._load()
            self._append("  Roster tab refreshed.\n")

    def run_raid_pull(self):
        def task():
            ok = self._run_script(RAID_PULL, "Raid Pull")
            if ok:
                self.after(0, self._reload_roster)
            self.after(0, self._set_running, False)
            self.after(0, self.status_var.set, "Ready")
        self._run_in_thread(task)

    def run_build_tracker(self):
        def task():
            self._run_script(BUILD_TRACKER, "Build Tracker")
            self.after(0, self._set_running, False)
            self.after(0, self.status_var.set, "Ready")
        self._run_in_thread(task)

    def run_both(self):
        def task():
            self._run_choice("Both (Chained)")
            self.after(0, self._set_running, False)
            self.after(0, self.status_var.set, "Ready")
        self._run_in_thread(task)

    def stop_process(self):
        global current_process
        with process_lock:
            if current_process and current_process.poll() is None:
                current_process.terminate()
                self._append("\n  Process terminated by user.\n")
                self.status_var.set("Stopped")

    # ── Built-in timer ──

    def _start_timer(self):
        try:
            hours = int(self.timer_hours.get())
        except ValueError:
            messagebox.showerror("Error", "Enter a valid number of hours.")
            return

        ms = hours * 3600 * 1000
        choice = self.timer_script.get()
        self.timer_status.configure(
            text=f"Timer: every {hours}h — {choice}", foreground="green")
        self.btn_timer_start.configure(state=tk.DISABLED)
        self.btn_timer_stop.configure(state=tk.NORMAL)

        def fire():
            def task():
                self._set_running(True)
                self._run_choice(choice)
                self.after(0, self._set_running, False)
                self.after(0, self.status_var.set, "Ready")
            self._run_in_thread(task)
            self.timer_id = self.after(ms, fire)

        # First fire after the interval
        self.timer_id = self.after(ms, fire)

    def _stop_timer(self):
        if self.timer_id:
            self.after_cancel(self.timer_id)
            self.timer_id = None
        self.timer_status.configure(text="Timer: Off", foreground="")
        self.btn_timer_start.configure(state=tk.NORMAL)
        self.btn_timer_stop.configure(state=tk.DISABLED)

    # ── Windows Task Scheduler ──

    def _build_task_command(self):
        """Build the command string that Task Scheduler will execute.
        Writes a .bat with the actual commands, then a .vbs that launches
        the .bat with a hidden window (no visible cmd.exe)."""
        choice = self.sched_script.get()

        if IS_FROZEN:
            # .exe scripts — run directly
            if choice == "Raid Pull":
                lines = [f'"{RAID_PULL}"']
            elif choice == "Build Tracker":
                lines = [f'"{BUILD_TRACKER}"']
            else:
                lines = [
                    f'"{RAID_PULL}"',
                    f'if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%',
                    f'"{BUILD_TRACKER}"',
                ]
        else:
            # .py scripts — run via Python interpreter
            python = sys.executable
            if choice == "Raid Pull":
                lines = [f'"{python}" "{RAID_PULL}"']
            elif choice == "Build Tracker":
                lines = [f'"{python}" "{BUILD_TRACKER}"']
            else:
                lines = [
                    f'"{python}" "{RAID_PULL}"',
                    f'if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%',
                    f'"{python}" "{BUILD_TRACKER}"',
                ]

        # .bat with the actual commands
        bat_path = os.path.join(SCRIPT_DIR, "scheduled_task.bat")
        with open(bat_path, "w", encoding="utf-8") as bf:
            bf.write("@echo off\n")
            bf.write(f'cd /d "{SCRIPT_DIR}"\n')
            for line in lines:
                bf.write(line + "\n")

        # .vbs wrapper — launches the .bat with hidden window (0)
        vbs_path = os.path.join(SCRIPT_DIR, "scheduled_task.vbs")
        with open(vbs_path, "w", encoding="utf-8") as vf:
            vf.write(f'CreateObject("WScript.Shell").Run """{bat_path}""", 0, True\n')

        return f'wscript.exe "{vbs_path}"'

    def _register_task(self):
        day_map = {
            "Monday": "MON", "Tuesday": "TUE", "Wednesday": "WED",
            "Thursday": "THU", "Friday": "FRI", "Saturday": "SAT",
            "Sunday": "SUN",
        }
        day = day_map[self.sched_day.get()]
        hour = self.sched_hour.get().zfill(2)
        minute = self.sched_min.get().zfill(2)
        time_str = f"{hour}:{minute}"
        cmd = self._build_task_command()

        schtasks_cmd = [
            "schtasks", "/create",
            "/tn", TASK_NAME,
            "/tr", cmd,
            "/sc", "weekly",
            "/d", day,
            "/st", time_str,
            "/f",  # force overwrite if exists
        ]

        try:
            run_kwargs = {}
            if sys.platform == "win32":
                run_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            result = subprocess.run(schtasks_cmd, capture_output=True,
                                    text=True, **run_kwargs)
            if result.returncode == 0:
                self.sched_status.configure(
                    text=f"Registered: {self.sched_day.get()} at {time_str}",
                    foreground="green")
                self._append(f"\nTask Scheduler: registered for "
                             f"{self.sched_day.get()} at {time_str}\n")
            else:
                self.sched_status.configure(text="Failed to register",
                                            foreground="red")
                self._append(f"\nTask Scheduler error:\n{result.stderr}\n")
        except Exception as ex:
            self.sched_status.configure(text="Error", foreground="red")
            self._append(f"\nTask Scheduler exception: {ex}\n")

    def _remove_task(self):
        try:
            run_kwargs = {}
            if sys.platform == "win32":
                run_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            result = subprocess.run(
                ["schtasks", "/delete", "/tn", TASK_NAME, "/f"],
                capture_output=True, text=True, **run_kwargs)
            if result.returncode == 0:
                self.sched_status.configure(text="Schedule removed",
                                            foreground="")
                self._append("\nTask Scheduler: schedule removed.\n")
                # Clean up .bat and .vbs wrappers
                for _fn in ("scheduled_task.bat", "scheduled_task.vbs"):
                    _fp = os.path.join(SCRIPT_DIR, _fn)
                    if os.path.isfile(_fp):
                        os.remove(_fp)
            else:
                self.sched_status.configure(text="No schedule found",
                                            foreground="")
        except Exception as ex:
            self._append(f"\nTask Scheduler exception: {ex}\n")

    def _check_task_status(self):
        """Check if a task is already registered on startup."""
        try:
            run_kwargs = {}
            if sys.platform == "win32":
                run_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            result = subprocess.run(
                ["schtasks", "/query", "/tn", TASK_NAME],
                capture_output=True, text=True, **run_kwargs)
            if result.returncode == 0:
                self.sched_status.configure(
                    text="Schedule active (registered)", foreground="green")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  Tab: Roster
# ═══════════════════════════════════════════════════════════════

class RosterTab(ttk.Frame):
    MIN_ROWS = 25
    STARTING_ALT_COLS = 4

    def __init__(self, parent):
        super().__init__(parent)
        self.alt_col_count = self.STARTING_ALT_COLS
        self.row_entries = []  # list of lists: [player, main, alt1, alt2, ...]
        self._row_frames = []  # parallel: row Frame widgets
        self._row_labels = []  # parallel: row number Label widgets
        self.excluded_chars = []  # characters excluded from roster
        self._build_ui()
        self._load()

    def _build_ui(self):
        # ── Team info (auto-filled from Config tab) ──
        team_frame = ttk.LabelFrame(self, text="Team Identity (auto-filled from Config)")
        team_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        # Center the fields using a sub-frame
        team_inner = ttk.Frame(team_frame)
        team_inner.pack(anchor=tk.CENTER, padx=10, pady=5)

        self.team_entries = {}
        team_fields = [
            ("Guild Name:", "guild_name"),
            ("Team Name:", "team_name"),
            ("Server:", "server"),
            ("Region:", "region"),
        ]
        for i, (label, key) in enumerate(team_fields):
            ttk.Label(team_inner, text=label).grid(row=0, column=i * 2,
                                                    sticky=tk.E, padx=(10, 3))
            entry = ttk.Entry(team_inner, width=18)
            entry.grid(row=0, column=i * 2 + 1, padx=(0, 10))
            self.team_entries[key] = entry

        # ── Explainer ──
        help_frame = ttk.Frame(self)
        help_frame.pack(fill=tk.X, padx=10, pady=(5, 2))

        help_text = (
            "These fields auto-fill from the Config tab. Edit them in Config "
            "to change globally.\n\n"
            "Roster: After the first Raid Pull, all characters appear in the "
            "unlinked list. Create player rows and assign mains/alts. Exact "
            "spelling and special characters matter — copy names directly from "
            "WoW or WCL to avoid mismatches (e.g. Constånce not Constance).\n\n"
            "Lock Roster: When locked, unrostered players are still scored "
            "individually but excluded from raid averages."
        )
        help_label = tk.Label(help_frame, text=help_text, wraplength=1000,
                              justify=tk.LEFT, anchor=tk.W, fg="#555555",
                              font=("Segoe UI", 10))
        help_label.pack(fill=tk.X)

        # ── Grid controls ──
        ctrl_frame = ttk.Frame(self)
        ctrl_frame.pack(fill=tk.X, padx=10, pady=(2, 0))

        ttk.Button(ctrl_frame, text="+ Add Alt Column",
                   command=self._add_alt_column).pack(side=tk.LEFT, padx=5)
        ttk.Button(ctrl_frame, text="+ Add Row",
                   command=self._add_row).pack(side=tk.LEFT, padx=5)
        ttk.Button(ctrl_frame, text="Save Roster",
                   command=self._save).pack(side=tk.LEFT, padx=20)
        self.save_label = ttk.Label(ctrl_frame, text="")
        self.save_label.pack(side=tk.LEFT, padx=5)

        self.rebuild_btn = ttk.Button(
            ctrl_frame, text="\u21bb Rebuild Report",
            command=self._rebuild_report, state=tk.DISABLED)
        self.rebuild_btn.pack(side=tk.LEFT, padx=10)
        Tooltip(self.rebuild_btn, lambda: (
            "Re-runs the Build Tracker using existing data\n"
            "with your updated roster — no new API pulls needed."))

        # Lock toggle — right side
        self.locked_var = tk.BooleanVar(value=False)
        self.locked_var.trace_add("write", lambda *_: self._update_lock_hint())
        lock_frame = ttk.Frame(ctrl_frame)
        lock_frame.pack(side=tk.RIGHT, padx=10)
        self.lock_cb = ttk.Checkbutton(
            lock_frame, text="Lock Roster",
            variable=self.locked_var)
        self.lock_cb.pack(side=tk.LEFT)
        self.lock_hint = ttk.Label(
            lock_frame, text="",
            font=("Segoe UI", 10), foreground="#555555")
        self.lock_hint.pack(side=tk.LEFT, padx=(5, 0))

        # ── Scrollable grid area ──
        grid_container = ttk.Frame(self)
        grid_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.canvas = tk.Canvas(grid_container)
        v_scroll = ttk.Scrollbar(grid_container, orient=tk.VERTICAL,
                                  command=self.canvas.yview)
        h_scroll = ttk.Scrollbar(grid_container, orient=tk.HORIZONTAL,
                                  command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=v_scroll.set,
                              xscrollcommand=h_scroll.set)

        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.grid_frame = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0),
                                                        window=self.grid_frame,
                                                        anchor=tk.NW)
        self.grid_frame.bind("<Configure>", self._on_grid_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        # Build headers and rows
        self._build_headers()
        for _ in range(self.MIN_ROWS):
            self._add_row_widgets()

        # ── Excluded characters section (scrollable) ──
        self.excluded_frame = ttk.LabelFrame(self, text="Excluded Characters (not scored, won't re-add on pull)")
        self.excluded_frame.pack(fill=tk.BOTH, padx=10, pady=(5, 10))

        self.excluded_canvas = tk.Canvas(self.excluded_frame, height=120)
        excl_scroll = ttk.Scrollbar(self.excluded_frame, orient=tk.VERTICAL,
                                     command=self.excluded_canvas.yview)
        self.excluded_canvas.configure(yscrollcommand=excl_scroll.set)
        excl_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.excluded_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.excluded_inner = ttk.Frame(self.excluded_canvas)
        self.excluded_canvas_window = self.excluded_canvas.create_window(
            (0, 0), window=self.excluded_inner, anchor=tk.NW)
        self.excluded_inner.bind("<Configure>",
            lambda e: self.excluded_canvas.configure(
                scrollregion=self.excluded_canvas.bbox("all")))
        self.excluded_canvas.bind("<Configure>",
            lambda e: self.excluded_canvas.itemconfig(
                self.excluded_canvas_window, width=e.width))

    def _on_grid_configure(self, event):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        # Only expand to fill canvas — don't shrink if content is wider
        min_width = self.grid_frame.winfo_reqwidth()
        self.canvas.itemconfig(self.canvas_window,
                               width=max(event.width, min_width))

    def _build_headers(self):
        self._header_frame = ttk.Frame(self.grid_frame)
        self._header_frame.pack(fill=tk.X)
        headers = ["#", "Player", "Main"]
        for i in range(self.alt_col_count):
            headers.append(f"Alt {i + 1}")
        headers.append("")  # X button column

        widths = [3, 16, 16] + [16] * self.alt_col_count + [3]
        for col, (text, w) in enumerate(zip(headers, widths)):
            lbl = ttk.Label(self._header_frame, text=text, width=w,
                            font=("Segoe UI", 11, "bold"))
            lbl.pack(side=tk.LEFT, padx=2, pady=2)

    def _rebuild_headers(self):
        """Clear and rebuild headers after adding a column."""
        if hasattr(self, "_header_frame"):
            self._header_frame.destroy()
        self._build_headers()

    def _add_row_widgets(self, values=None):
        """Add one row as its own frame. values = [player, main, alt1, ...]"""
        row_num = len(self.row_entries) + 1

        row_frame = ttk.Frame(self.grid_frame)
        row_frame.pack(fill=tk.X)

        # Row number label
        lbl = ttk.Label(row_frame, text=str(row_num), width=3)
        lbl.pack(side=tk.LEFT, padx=2, pady=1)

        # Player + Main + alt columns
        total_cols = 2 + self.alt_col_count
        entries = []
        for col in range(total_cols):
            entry = ttk.Entry(row_frame, width=16)
            entry.pack(side=tk.LEFT, padx=2, pady=1)
            if values and col < len(values):
                entry.insert(0, values[col])
            entries.append(entry)

        # X (exclude) button
        x_btn = ttk.Button(row_frame, text="\u2716", width=2,
                           command=lambda rf=row_frame, e=entries: self._exclude_row(rf, e))
        x_btn.pack(side=tk.LEFT, padx=2, pady=1)

        self.row_entries.append(entries)
        self._row_frames.append(row_frame)
        self._row_labels.append(lbl)

    def _add_row(self):
        """Add one empty row at the bottom."""
        self._add_row_widgets()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _add_alt_column(self):
        """Add one more alt column to every row (rare — full rebuild is fine)."""
        # Snapshot current values
        all_vals = []
        for entries in self.row_entries:
            all_vals.append([e.get().strip() for e in entries])
        self.alt_col_count += 1
        # Clear rows first, then headers, then rebuild (pack order matters)
        self._clear_grid()
        self._rebuild_headers()
        for v in all_vals:
            self._add_row_widgets(v)
        while len(self.row_entries) < self.MIN_ROWS:
            self._add_row_widgets()
        self.grid_frame.update_idletasks()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        self.canvas.itemconfig(self.canvas_window,
                               width=self.grid_frame.winfo_reqwidth())

    def _clear_grid(self):
        """Remove all row frames (not headers)."""
        for frame in self._row_frames:
            frame.destroy()
        self.row_entries.clear()
        self._row_frames.clear()
        self._row_labels.clear()

    def _exclude_row(self, row_frame, entries):
        """Move character names to excluded, destroy the row frame. Instant."""
        vals = [e.get().strip() for e in entries]
        chars = [v for v in vals[1:] if v]
        for c in chars:
            if c not in self.excluded_chars:
                self.excluded_chars.append(c)

        # Find index and remove from tracking lists
        try:
            idx = self._row_frames.index(row_frame)
        except ValueError:
            return
        self.row_entries.pop(idx)
        self._row_frames.pop(idx)
        self._row_labels.pop(idx)

        # Destroy the entire row frame — pack collapses automatically
        row_frame.destroy()

        # Renumber remaining rows
        for i, lbl in enumerate(self._row_labels):
            lbl.configure(text=str(i + 1))

        for c in chars:
            self._add_excluded_widget(c)

    def _restore_char(self, char_name):
        """Move a character from excluded back to a new player entry row."""
        if char_name in self.excluded_chars:
            self.excluded_chars.remove(char_name)
        self._remove_excluded_widget(char_name)
        # Add as a new row: player=char, main="", alt1=char
        self._add_row_widgets([char_name, "", char_name])
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _refresh_excluded_display(self):
        """Full rebuild of excluded section (used on load only)."""
        for widget in self.excluded_inner.winfo_children():
            widget.destroy()
        self._excluded_widgets = {}  # char → row_frame
        if not self.excluded_chars:
            self._excl_none_label = ttk.Label(self.excluded_inner, text="(none)",
                                               foreground="#999999")
            self._excl_none_label.pack(anchor=tk.W)
            return
        self._excl_none_label = None
        for char in sorted(self.excluded_chars):
            self._add_excluded_widget(char)

    def _add_excluded_widget(self, char):
        """Add a single excluded character row."""
        # Remove "(none)" placeholder if present
        if getattr(self, "_excl_none_label", None):
            self._excl_none_label.destroy()
            self._excl_none_label = None
        row_frame = ttk.Frame(self.excluded_inner)
        row_frame.pack(fill=tk.X, pady=1)
        ttk.Label(row_frame, text=char, width=20).pack(side=tk.LEFT)
        ttk.Button(row_frame, text="\u21a9 Restore", width=8,
                   command=lambda c=char: self._restore_char(c)
                   ).pack(side=tk.LEFT, padx=5)
        if not hasattr(self, "_excluded_widgets"):
            self._excluded_widgets = {}
        self._excluded_widgets[char] = row_frame

    def _remove_excluded_widget(self, char):
        """Remove a single excluded character row."""
        widgets = getattr(self, "_excluded_widgets", {})
        frame = widgets.pop(char, None)
        if frame:
            frame.destroy()
        if not self.excluded_chars:
            self._excl_none_label = ttk.Label(self.excluded_inner, text="(none)",
                                               foreground="#999999")
            self._excl_none_label.pack(anchor=tk.W)

    def _load(self):
        data = load_json(ROSTER_FILE, {"team": {}, "players": {}})

        # Locked flag
        locked = data.get("meta", {}).get("locked", False)
        self.locked_var.set(locked)
        self._update_lock_hint()

        # Excluded characters
        self.excluded_chars = list(data.get("excluded", []))

        # Team info: config.json is authoritative, roster.json is fallback
        cfg = load_json(CONFIG_FILE, {})
        roster_team = data.get("team", {})
        config_map = {
            "guild_name": cfg.get("guild_name", ""),
            "team_name": cfg.get("team_name", ""),
            "server": cfg.get("server", ""),
            "region": cfg.get("region", ""),
        }
        defaults = {"guild_name": "", "team_name": "", "server": "", "region": "US"}
        for key, entry in self.team_entries.items():
            val = (config_map.get(key, "")
                   or roster_team.get(key, "")
                   or defaults.get(key, ""))
            entry.delete(0, tk.END)
            entry.insert(0, val)

        # Players — figure out max alt count
        players = data.get("players", {})
        max_alts = self.STARTING_ALT_COLS
        for pdata in players.values():
            alt_count = len(pdata.get("alts", []))
            if alt_count > max_alts:
                max_alts = alt_count

        # Rebuild if we need more alt columns than default
        if max_alts > self.alt_col_count:
            self.alt_col_count = max_alts
            self._rebuild_headers()

        # Clear and re-populate rows
        self._clear_grid()
        for player_name, pdata in players.items():
            mains = pdata.get("mains", [])
            alts = pdata.get("alts", [])
            main_str = mains[0] if mains else ""
            values = [player_name, main_str] + alts
            self._add_row_widgets(values)

        # Pad to minimum rows
        while len(self.row_entries) < self.MIN_ROWS:
            self._add_row_widgets()

        self._refresh_excluded_display()

    def _save(self):
        # Team info
        team = {}
        for key, entry in self.team_entries.items():
            val = entry.get().strip()
            if val:
                team[key] = val

        # Players
        players = {}
        for entries in self.row_entries:
            vals = [e.get().strip() for e in entries]
            player_name = vals[0] if vals else ""
            if not player_name:
                continue  # skip blank rows

            main_str = vals[1] if len(vals) > 1 else ""
            mains = [main_str] if main_str else []
            alts = [v for v in vals[2:] if v]

            players[player_name] = {"mains": mains, "alts": alts}

        # If a char was restored into the roster, remove from excluded
        current_chars = set()
        for pdata in players.values():
            for c in pdata.get("mains", []):
                current_chars.add(c)
            for c in pdata.get("alts", []):
                current_chars.add(c)
        self.excluded_chars = [c for c in self.excluded_chars if c not in current_chars]

        data = {
            "meta": {
                "last_updated": __import__("datetime").datetime.now().strftime("%Y-%m-%d"),
                "source": "Raid Tools Launcher",
                "locked": self.locked_var.get()
            },
            "team": team,
            "players": players,
            "unlinked": [],
            "excluded": sorted(set(self.excluded_chars))
        }

        save_json(ROSTER_FILE, data)
        self._update_lock_hint()
        self._refresh_excluded_display()
        self.save_label.configure(text="Saved!", foreground="green")
        self.after(2000, lambda: self.save_label.configure(text=""))
        # Unlock rebuild button now that roster is saved
        self.rebuild_btn.configure(state=tk.NORMAL)

    def _rebuild_report(self):
        """Run Build Tracker via the Run tab's script runner (threaded, with popup)."""
        if hasattr(self, "_run_tab_ref") and self._run_tab_ref:
            self._run_tab_ref.run_build_tracker()
        else:
            from tkinter import messagebox
            messagebox.showwarning("Rebuild",
                                   "Cannot find Run tab reference.")

    def _update_lock_hint(self):
        locked = self.locked_var.get()
        if locked:
            self.lock_hint.configure(
                text="(Unrostered players excluded from raid averages)",
                foreground="#B8860B")
        else:
            self.lock_hint.configure(
                text="(All players count toward raid averages)",
                foreground="#555555")
        # Sync min_roster_players in config.json
        cfg = load_json(CONFIG_FILE, {})
        cfg["min_roster_players"] = 8 if locked else 0
        save_json(CONFIG_FILE, cfg)


# ═══════════════════════════════════════════════════════════════
#  Tab: Score Weights
# ═══════════════════════════════════════════════════════════════

class ScoreWeightsTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.weight_entries = {}
        self.grade_entries = {}
        self._build_ui()
        self._load()

    def _build_ui(self):
        # ── Scoring weights ──
        w_frame = ttk.LabelFrame(self, text="Scoring Weights (must total 100%)")
        w_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        weight_fields = [
            ("Mechanics:", "mechanics"),
            ("Deaths:", "deaths"),
            ("Parse Performance:", "parse_performance"),
            ("Consumables:", "consumables"),
        ]
        for i, (label, key) in enumerate(weight_fields):
            ttk.Label(w_frame, text=label).grid(row=i, column=0, sticky=tk.W,
                                                 padx=(10, 5), pady=3)
            entry = ttk.Entry(w_frame, width=8)
            entry.grid(row=i, column=1, sticky=tk.W, padx=5, pady=3)
            self.weight_entries[key] = entry

        self.weight_total_label = ttk.Label(w_frame, text="Total: —")
        self.weight_total_label.grid(row=len(weight_fields), column=0,
                                      columnspan=2, padx=10, pady=(5, 8),
                                      sticky=tk.W)

        # Bind live total update
        for entry in self.weight_entries.values():
            entry.bind("<KeyRelease>", self._update_total)

        # ── Grade thresholds ──
        g_frame = ttk.LabelFrame(self, text="Grade Thresholds (minimum score for each grade)")
        g_frame.pack(fill=tk.X, padx=10, pady=5)

        grade_fields = [("A:", "A"), ("B:", "B"), ("C:", "C"),
                        ("D:", "D"), ("F:", "F")]
        for i, (label, key) in enumerate(grade_fields):
            ttk.Label(g_frame, text=label).grid(row=0, column=i * 2,
                                                 sticky=tk.E, padx=(10, 3),
                                                 pady=8)
            entry = ttk.Entry(g_frame, width=6)
            entry.grid(row=0, column=i * 2 + 1, padx=(0, 10), pady=8)
            self.grade_entries[key] = entry

        # ── Scoring methods reference ──
        ref_frame = ttk.LabelFrame(self, text="Scoring Methods Reference (read-only)")
        ref_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.methods_text = tk.Text(ref_frame, wrap=tk.WORD, height=12,
                                     font=("Segoe UI", 11), state=tk.DISABLED)
        self.methods_text.tag_configure("header",
                                         font=("Segoe UI", 11, "bold"))
        self.methods_text.tag_configure("desc",
                                         font=("Segoe UI", 11),
                                         lmargin1=15, lmargin2=15)
        self.methods_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # ── Save button ──
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="Save Weights",
                   command=self._save).pack(side=tk.LEFT)
        self.save_label = ttk.Label(btn_frame, text="")
        self.save_label.pack(side=tk.LEFT, padx=10)

    def _update_total(self, event=None):
        total = 0.0
        valid = True
        for entry in self.weight_entries.values():
            try:
                total += float(entry.get())
            except ValueError:
                valid = False
                break

        if valid:
            pct = total * 100
            color = "green" if abs(total - 1.0) < 0.001 else "red"
            self.weight_total_label.configure(
                text=f"Total: {pct:.0f}%", foreground=color)
        else:
            self.weight_total_label.configure(
                text="Total: (invalid input)", foreground="red")

    def _load(self):
        data = load_json(MECHANIC_FILE, {})

        weights = data.get("scoring_weights", {})
        defaults = {"mechanics": 0.4, "deaths": 0.35,
                    "parse_performance": 0.2, "consumables": 0.05}
        for key, entry in self.weight_entries.items():
            entry.delete(0, tk.END)
            entry.insert(0, str(weights.get(key, defaults.get(key, 0))))

        grades = data.get("grade_thresholds", {})
        grade_defaults = {"A": 90, "B": 80, "C": 70, "D": 60, "F": 0}
        for key, entry in self.grade_entries.items():
            entry.delete(0, tk.END)
            entry.insert(0, str(grades.get(key, grade_defaults.get(key, 0))))

        # Scoring methods reference — always use built-in definitions
        self.methods_text.configure(state=tk.NORMAL)
        self.methods_text.delete("1.0", tk.END)
        for method, desc in SCORING_METHODS.items():
            self.methods_text.insert(tk.END, f"{method}\n", "header")
            self.methods_text.insert(tk.END, f"{desc}\n\n", "desc")
        self.methods_text.configure(state=tk.DISABLED)

        self._update_total()

    def _save(self):
        # Validate weights
        weights = {}
        for key, entry in self.weight_entries.items():
            try:
                weights[key] = float(entry.get())
            except ValueError:
                messagebox.showerror("Error",
                                     f"Invalid weight for {key}.")
                return

        total = sum(weights.values())
        if abs(total - 1.0) > 0.001:
            pct = total * 100
            if not messagebox.askyesno(
                    "Warning",
                    f"Weights total {pct:.0f}%, not 100%.\n"
                    "Save anyway?"):
                return

        # Validate grades
        grades = {}
        for key, entry in self.grade_entries.items():
            try:
                grades[key] = int(entry.get())
            except ValueError:
                messagebox.showerror("Error",
                                     f"Invalid threshold for grade {key}.")
                return

        # Load existing file, update only the weight/grade sections
        data = load_json(MECHANIC_FILE, {})
        data["scoring_weights"] = weights
        data["grade_thresholds"] = grades

        save_json(MECHANIC_FILE, data)
        self.save_label.configure(text="Saved!", foreground="green")
        self.after(2000, lambda: self.save_label.configure(text=""))


# ═══════════════════════════════════════════════════════════════
#  Tooltip helper
# ═══════════════════════════════════════════════════════════════

class Tooltip:
    """Hover tooltip for any widget."""
    def __init__(self, widget, text_func):
        self.widget = widget
        self.text_func = text_func  # callable that returns tooltip text
        self.tip_window = None
        widget.bind("<Enter>", self._show)
        widget.bind("<Leave>", self._hide)

    def _show(self, event):
        text = self.text_func()
        if not text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 5
        self._create_tip(x, y, text)

    def _create_tip(self, x, y, text):
        self._hide(None)
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=text, justify=tk.LEFT, wraplength=500,
                         background="#ffffe0", relief=tk.SOLID, borderwidth=1,
                         font=("Segoe UI", 10))
        label.pack()

    def _hide(self, event):
        if self.tip_window:
            self.tip_window.destroy()
            self.tip_window = None


class MethodPicker(ttk.Frame):
    """Custom dropdown that shows a description tooltip when hovering
    each item in the open list.  Looks like a Combobox but gives us
    full control over the popup listbox."""

    def __init__(self, parent, values, descs, width=15, **kw):
        super().__init__(parent, **kw)
        self._values = values
        self._descs = descs      # {value: description}
        self._current = tk.StringVar()
        self._popup = None
        self._tip = None

        self._btn = ttk.Combobox(self, textvariable=self._current,
                                  values=values, state="readonly", width=width)
        self._btn.pack(fill=tk.X)
        # Intercept the dropdown open: close the native one, show ours
        self._btn.bind("<<ComboboxSelected>>", self._on_native_select)
        self._btn.bind("<Button-1>", self._on_click)

    # ── Public interface (matches Entry/Combobox) ──
    def get(self):
        return self._current.get()

    def set(self, value):
        self._current.set(value)

    def grid(self, **kw):
        super().grid(**kw)

    def pack(self, **kw):
        super().pack(**kw)

    def _on_native_select(self, event):
        """If the native dropdown fires, just accept it."""
        self._close_popup()

    def _on_click(self, event):
        """Open our custom popup instead of the native dropdown."""
        # Let the native dropdown open, then immediately close it and
        # open ours.  Schedule so native event completes first.
        self.after(10, self._open_popup)

    def _open_popup(self):
        # Close native dropdown if open
        try:
            self._btn.event_generate("<Escape>")
        except Exception:
            pass

        if self._popup and self._popup.winfo_exists():
            self._close_popup()
            return

        self._popup = popup = tk.Toplevel(self)
        popup.wm_overrideredirect(True)
        popup.wm_attributes("-topmost", True)

        # Position below the button
        x = self._btn.winfo_rootx()
        y = self._btn.winfo_rooty() + self._btn.winfo_height()
        popup.wm_geometry(f"+{x}+{y}")

        lb = tk.Listbox(popup, selectmode=tk.SINGLE, activestyle="none",
                         font=("Segoe UI", 11), highlightthickness=1,
                         relief=tk.SOLID, borderwidth=1)
        for v in self._values:
            lb.insert(tk.END, v)

        # Pre-select current value
        cur = self._current.get()
        if cur in self._values:
            idx = self._values.index(cur)
            lb.selection_set(idx)
            lb.see(idx)

        lb.pack(fill=tk.BOTH, expand=True)
        # Size the listbox to fit content
        lb.configure(height=min(len(self._values), 12),
                     width=max(len(v) for v in self._values) + 2)

        lb.bind("<Motion>", self._on_motion)
        lb.bind("<Leave>", self._on_leave)
        lb.bind("<ButtonRelease-1>", self._on_select)
        popup.bind("<Escape>", lambda e: self._close_popup())
        popup.bind("<FocusOut>", lambda e: self.after(100, self._check_focus))
        self._lb = lb

        # Close if user clicks elsewhere
        popup.grab_set()

    def _check_focus(self):
        """Close popup if focus left entirely."""
        try:
            if self._popup and self._popup.winfo_exists():
                focused = self._popup.focus_get()
                if focused is None:
                    self._close_popup()
        except Exception:
            self._close_popup()

    def _on_motion(self, event):
        idx = self._lb.nearest(event.y)
        self._lb.selection_clear(0, tk.END)
        self._lb.selection_set(idx)

        val = self._lb.get(idx)
        desc = self._descs.get(val, "")
        if desc:
            # Show tooltip to the right of the popup
            tip_x = self._popup.winfo_rootx() + self._popup.winfo_width() + 5
            tip_y = self._popup.winfo_rooty() + event.y - 5
            self._show_tip(tip_x, tip_y, f"{val}\n{desc}")
        else:
            self._hide_tip()

    def _on_leave(self, event):
        self._hide_tip()

    def _on_select(self, event):
        idx = self._lb.curselection()
        if idx:
            self._current.set(self._lb.get(idx[0]))
        self._close_popup()

    def _show_tip(self, x, y, text):
        self._hide_tip()
        self._tip = tw = tk.Toplevel(self)
        tw.wm_overrideredirect(True)
        tw.wm_attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=text, justify=tk.LEFT, wraplength=450,
                         background="#ffffe0", relief=tk.SOLID, borderwidth=1,
                         font=("Segoe UI", 10))
        label.pack()

    def _hide_tip(self):
        if self._tip:
            try:
                self._tip.destroy()
            except Exception:
                pass
            self._tip = None

    def _close_popup(self):
        self._hide_tip()
        if self._popup:
            try:
                self._popup.grab_release()
                self._popup.destroy()
            except Exception:
                pass
            self._popup = None


class AbilityPicker(ttk.Frame):
    """Editable entry with dropdown + autocomplete + per-item tooltips.
    Typing filters the list in real time.  Click the arrow or start typing
    to open.  Free-text still works for abilities not in the list."""

    def __init__(self, parent, width=18, **kw):
        super().__init__(parent, **kw)
        self._values = []
        self._filtered = []
        self._descs = {}         # {ability_name: description}
        self._popup = None
        self._tip = None
        self._lb = None
        self._select_callback = None  # called with (ability_name) on pick

        self._entry = ttk.Entry(self, width=width)
        self._entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._entry.bind("<KeyRelease>", self._on_key)
        self._entry.bind("<FocusOut>", self._on_entry_focusout)
        self._entry.bind("<Escape>", lambda e: self._close_popup())

        self._drop_btn = ttk.Button(self, text="\u25bc", width=2,
                                     command=self._toggle_popup)
        self._drop_btn.pack(side=tk.LEFT)

    # ── Public interface ──
    def get(self):
        return self._entry.get()

    def set(self, value):
        self._entry.delete(0, tk.END)
        self._entry.insert(0, value)

    def set_choices(self, values, descs=None):
        """Update dropdown values and descriptions."""
        self._values = list(values)
        self._descs = descs or {}

    # ── Autocomplete ──
    def _on_key(self, event):
        # Ignore navigation/modifier keys
        if event.keysym in ("Escape", "Return", "Tab", "Shift_L",
                            "Shift_R", "Control_L", "Control_R",
                            "Alt_L", "Alt_R", "Up", "Down"):
            if event.keysym == "Return" and self._popup:
                # Accept highlighted item
                if self._lb:
                    sel = self._lb.curselection()
                    if sel:
                        self._entry.delete(0, tk.END)
                        self._entry.insert(0, self._lb.get(sel[0]))
                self._close_popup()
                return
            if event.keysym == "Down" and self._lb:
                # Move selection down in popup
                sel = self._lb.curselection()
                idx = (sel[0] + 1) if sel else 0
                if idx < self._lb.size():
                    self._lb.selection_clear(0, tk.END)
                    self._lb.selection_set(idx)
                    self._lb.see(idx)
                return
            if event.keysym == "Up" and self._lb:
                sel = self._lb.curselection()
                idx = (sel[0] - 1) if sel else 0
                if idx >= 0:
                    self._lb.selection_clear(0, tk.END)
                    self._lb.selection_set(idx)
                    self._lb.see(idx)
                return
            return

        if not self._values:
            return

        typed = self._entry.get().strip().lower()
        if not typed:
            self._filtered = self._values[:]
        else:
            # Show items that contain the typed text, prioritising starts-with
            starts = [v for v in self._values if v.lower().startswith(typed)]
            contains = [v for v in self._values
                        if typed in v.lower() and v not in starts]
            self._filtered = starts + contains

        if self._filtered:
            self._update_popup()
        else:
            self._close_popup()

    def _on_entry_focusout(self, event):
        # Delay so click on popup can register first
        self.after(150, self._check_close)

    def _check_close(self):
        try:
            focused = self.focus_get()
            # Keep popup open if focus went to the popup
            if self._popup and self._popup.winfo_exists():
                if focused and (focused == self._lb or
                                str(focused).startswith(str(self._popup))):
                    return
            self._close_popup()
        except Exception:
            self._close_popup()

    # ── Dropdown ──
    def _toggle_popup(self):
        if self._popup and self._popup.winfo_exists():
            self._close_popup()
        else:
            self._filtered = self._values[:]
            self._update_popup()

    def _update_popup(self):
        """Open or refresh the popup with current filtered list."""
        if not self._filtered:
            self._close_popup()
            return

        if self._popup and self._popup.winfo_exists():
            # Just update the listbox contents
            self._lb.delete(0, tk.END)
            for v in self._filtered:
                self._lb.insert(tk.END, v)
            visible = min(len(self._filtered), 15)
            self._lb.configure(height=visible)
            return

        # Create new popup
        self._popup = popup = tk.Toplevel(self)
        popup.wm_overrideredirect(True)
        popup.wm_attributes("-topmost", True)

        x = self._entry.winfo_rootx()
        y = self._entry.winfo_rooty() + self._entry.winfo_height()

        frame = ttk.Frame(popup)
        frame.pack(fill=tk.BOTH, expand=True)

        sb = ttk.Scrollbar(frame, orient=tk.VERTICAL)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        lb = tk.Listbox(frame, selectmode=tk.SINGLE, activestyle="none",
                         font=("Segoe UI", 11), highlightthickness=1,
                         relief=tk.SOLID, borderwidth=1,
                         yscrollcommand=sb.set)
        lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.configure(command=lb.yview)

        for v in self._filtered:
            lb.insert(tk.END, v)

        visible = min(len(self._filtered), 15)
        max_w = max((len(v) for v in self._filtered), default=20) + 4
        lb.configure(height=visible, width=min(max_w, 35))

        popup.wm_geometry(f"+{x}+{y}")

        lb.bind("<Motion>", self._on_motion)
        lb.bind("<Leave>", self._on_leave)
        lb.bind("<ButtonRelease-1>", self._on_select)
        lb.bind("<MouseWheel>",
                lambda e: lb.yview_scroll(int(-1 * (e.delta / 120)), "units"))
        self._lb = lb

        # Do NOT grab — let the entry keep focus for typing

    def _on_motion(self, event):
        idx = self._lb.nearest(event.y)
        self._lb.selection_clear(0, tk.END)
        self._lb.selection_set(idx)

        val = self._lb.get(idx)
        desc = self._descs.get(val, "")
        if desc:
            tip_x = self._popup.winfo_rootx() + self._popup.winfo_width() + 5
            tip_y = self._popup.winfo_rooty() + event.y - 5
            self._show_tip(tip_x, tip_y, f"{val}\n{desc}")
        else:
            self._hide_tip()

    def _on_leave(self, event):
        self._hide_tip()

    def _on_select(self, event):
        idx = self._lb.curselection()
        if idx:
            val = self._lb.get(idx[0])
            self._entry.delete(0, tk.END)
            self._entry.insert(0, val)
            if self._select_callback:
                self._select_callback(val)
        self._close_popup()
        # Return focus to entry
        self._entry.focus_set()

    def _show_tip(self, x, y, text):
        self._hide_tip()
        self._tip = tw = tk.Toplevel(self)
        tw.wm_overrideredirect(True)
        tw.wm_attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=text, justify=tk.LEFT, wraplength=450,
                         background="#ffffe0", relief=tk.SOLID, borderwidth=1,
                         font=("Segoe UI", 10))
        label.pack()

    def _hide_tip(self):
        if self._tip:
            try:
                self._tip.destroy()
            except Exception:
                pass
            self._tip = None

    def _close_popup(self):
        self._hide_tip()
        if self._popup:
            try:
                self._popup.destroy()
            except Exception:
                pass
            self._popup = None
            self._lb = None


# ═══════════════════════════════════════════════════════════════
#  Tab: Mechanics
# ═══════════════════════════════════════════════════════════════

# Scoring method descriptions for tooltips
SCORING_METHODS = {
    "binary_fail": "ANY damage taken = fail (penalty). Most avoidable mechanics.",
    "relative_fail": "Damage taken ABOVE raid median = fail (scaled penalty). Overlap/positioning.",
    "binary_pass": "ANY damage taken = you participated (bonus). Soaks, orb intercepts.",
    "target_swap": "Target damage > 0 = you swapped to adds (bonus).",
    "bonus": "Special positive action detected (e.g. Fracture ghost pickup).",
    "conditional_fail": "Fail only if a trigger condition is met (e.g. Excess Nova fired).",
    "immune_soak": "Damage > 0 = pass. Immune classes (Mage/Rogue/Paladin) with 0 = pass.",
    "ignore": "Not scored.",
    "tank_swap_binary": "Debuff with huge multiplier, must swap every cast.",
    "tank_swap_ratio": "Continuously stacking debuff, compare distribution between tanks.",
}

METHOD_LIST = list(SCORING_METHODS.keys())
ROLE_FILTERS = ["", "non_tank", "tank_only", "healer_only"]
TANK_METHODS = ["binary", "ratio"]
TANK_METHOD_DESCS = {
    "binary": "Must swap every cast. Co-tank apps beyond max_safe = fail.",
    "ratio": "Compare stack distribution. Ratio above threshold = fail.",
}


class MechanicsTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.boss_data = {}      # full bosses dict from JSON
        self.current_boss = None
        self.mech_rows = []
        self.tank_rows = []
        self.target_rows = []
        self.bonus_rows = []
        self.probe_cache = {}    # loaded from probe_cache.json
        self._load_probe_cache()
        self._build_ui()
        self._load_all()

    def _load_probe_cache(self):
        self.probe_cache = load_json(PROBE_CACHE, {})

    def _get_ability_names(self, boss_name=None):
        """Get ability names for current boss from mechanic_rulesets.json only.
        Never reads from probe_cache — the probe writes to rulesets,
        and this tab reads from rulesets."""
        if not boss_name:
            return []
        boss_data = self.boss_data.get(boss_name, {})
        names = set()
        for ability in boss_data.get("mechanics", {}):
            names.add(ability)
        for rule in boss_data.get("tank_swap_rules", []):
            if rule.get("debuff"):
                names.add(rule["debuff"])
        for ability in boss_data.get("bonus_mechanics", {}):
            names.add(ability)
        for add_name in boss_data.get("target_swap", {}):
            names.add(add_name)
        for name in boss_data.get("ignored", []):
            if name:
                names.add(name)
        return sorted(names)

    def _get_ability_descs(self, boss_name=None):
        """Build {ability_name: description} map for current boss.
        Reads from mechanic_rulesets.json display text only."""
        descs = {}
        if not boss_name:
            return descs
        boss_data = self.boss_data.get(boss_name, {})
        for ability, info in boss_data.get("mechanics", {}).items():
            d = info.get("display", "")
            if d:
                descs[ability] = d
        return descs

    def _make_ability_combo(self, parent, current="", width=18):
        """Create an AbilityPicker with autocomplete + per-item tooltips."""
        picker = AbilityPicker(parent, width=width)
        picker.set(current)
        names = self._get_ability_names(self.current_boss)
        descs = self._get_ability_descs(self.current_boss)
        picker.set_choices(names, descs)
        return picker

    def _build_ui(self):
        # ── Explainer ──
        help_frame = ttk.Frame(self)
        help_frame.pack(fill=tk.X, padx=10, pady=(10, 2))

        help_text = (
            "Configure scored mechanics per boss. Each mechanic has a name "
            "(exact WCL ability name), a weight (100% = normal importance, "
            "lower = less impact, up to 150% for critical mechanics), a "
            "scoring method (hover the dropdown to see what each does), and "
            "a fix description shown to raiders.\n\n"
            "Select a boss from the dropdown, edit its mechanics, then Save. "
            "Use Add/Remove Boss to set up new raid tiers. Run the WCL Probe "
            "from the Config tab to auto-discover ability names from top-ranked logs."
        )
        tk.Label(help_frame, text=help_text, wraplength=1000, justify=tk.LEFT,
                 anchor=tk.W, fg="#555555",
                 font=("Segoe UI", 10)).pack(fill=tk.X)

        # ── Boss selector bar ──
        boss_bar = ttk.Frame(self)
        boss_bar.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(boss_bar, text="Boss:").pack(side=tk.LEFT, padx=(0, 5))
        self.boss_combo = ttk.Combobox(boss_bar, state="readonly", width=25)
        self.boss_combo.pack(side=tk.LEFT, padx=5)
        self.boss_combo.bind("<<ComboboxSelected>>", self._on_boss_select)

        ttk.Label(boss_bar, text="Encounter ID:").pack(side=tk.LEFT,
                                                        padx=(15, 5))
        self.encounter_id_entry = ttk.Entry(boss_bar, width=14)
        self.encounter_id_entry.pack(side=tk.LEFT, padx=5)

        ttk.Button(boss_bar, text="Add Boss",
                   command=self._add_boss).pack(side=tk.LEFT, padx=10)
        ttk.Button(boss_bar, text="Remove Boss",
                   command=self._remove_boss).pack(side=tk.LEFT, padx=5)

        # ── Scrollable content area ──
        container = ttk.Frame(self)
        container.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.canvas = tk.Canvas(container)
        v_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL,
                                  command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=v_scroll.set)
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.inner = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0),
                                                        window=self.inner,
                                                        anchor=tk.NW)
        self.inner.bind("<Configure>",
                        lambda e: self.canvas.configure(
                            scrollregion=self.canvas.bbox("all")))
        self.canvas.bind("<Configure>",
                         lambda e: self.canvas.itemconfig(
                             self.canvas_window,
                             width=max(e.width,
                                       self.inner.winfo_reqwidth())))
        # Section frames (built inside inner)
        self._build_sections()

        # ── Save button ──
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=8)
        ttk.Button(btn_frame, text="Save Mechanics",
                   command=self._save).pack(side=tk.LEFT)
        self.save_label = ttk.Label(btn_frame, text="")
        self.save_label.pack(side=tk.LEFT, padx=10)

    def _build_sections(self):
        """Build the five section frames inside the scrollable area."""
        # ── Mechanics ──
        self.mech_frame = ttk.LabelFrame(self.inner, text="Mechanics")
        self.mech_frame.pack(fill=tk.X, padx=5, pady=5)

        self.mech_grid = ttk.Frame(self.mech_frame)
        self.mech_grid.pack(fill=tk.X, padx=2, pady=2)

        mech_headers = ["Ability Name", "Wt%", "Method", "Display Text",
                         "Fix", "Role", ""]
        for col, text in enumerate(mech_headers):
            ttk.Label(self.mech_grid, text=text,
                      font=("Segoe UI", 10, "bold")).grid(
                row=0, column=col, padx=2, pady=2, sticky=tk.W)
        self.mech_next_row = 1

        ttk.Button(self.mech_frame, text="+ Add Mechanic",
                   command=self._add_mech_row).pack(anchor=tk.W, padx=5, pady=3)

        # ── Tank Swap Rules ──
        self.tank_frame = ttk.LabelFrame(self.inner, text="Tank Swap Rules")
        self.tank_frame.pack(fill=tk.X, padx=5, pady=5)

        self.tank_grid = ttk.Frame(self.tank_frame)
        self.tank_grid.pack(fill=tk.X, padx=2, pady=2)

        tank_headers = ["Ability / Debuff", "Method", "Max Safe / Ratio",
                         "Display Text", "Fix", ""]
        for col, text in enumerate(tank_headers):
            ttk.Label(self.tank_grid, text=text,
                      font=("Segoe UI", 10, "bold")).grid(
                row=0, column=col, padx=2, pady=2, sticky=tk.W)
        self.tank_next_row = 1

        ttk.Button(self.tank_frame, text="+ Add Tank Swap",
                   command=self._add_tank_row).pack(anchor=tk.W, padx=5, pady=3)

        # ── Target Swaps ──
        self.target_frame = ttk.LabelFrame(self.inner,
                                            text="Target Swaps (Add Switching)")
        self.target_frame.pack(fill=tk.X, padx=5, pady=5)

        self.target_grid = ttk.Frame(self.target_frame)
        self.target_grid.pack(fill=tk.X, padx=2, pady=2)

        target_headers = ["Add Name", "Display Text", "Fix", ""]
        for col, text in enumerate(target_headers):
            ttk.Label(self.target_grid, text=text,
                      font=("Segoe UI", 10, "bold")).grid(
                row=0, column=col, padx=2, pady=2, sticky=tk.W)
        self.target_next_row = 1

        ttk.Button(self.target_frame, text="+ Add Target Swap",
                   command=self._add_target_row).pack(anchor=tk.W, padx=5,
                                                       pady=3)

        # ── Bonus Mechanics ──
        self.bonus_frame = ttk.LabelFrame(self.inner, text="Bonus Mechanics")
        self.bonus_frame.pack(fill=tk.X, padx=5, pady=5)

        self.bonus_grid = ttk.Frame(self.bonus_frame)
        self.bonus_grid.pack(fill=tk.X, padx=2, pady=2)

        bonus_headers = ["Ability Name", "Method", "Display Text",
                          "Fix", "Role", ""]
        for col, text in enumerate(bonus_headers):
            ttk.Label(self.bonus_grid, text=text,
                      font=("Segoe UI", 10, "bold")).grid(
                row=0, column=col, padx=2, pady=2, sticky=tk.W)
        self.bonus_next_row = 1

        ttk.Button(self.bonus_frame, text="+ Add Bonus Mechanic",
                   command=self._add_bonus_row).pack(anchor=tk.W, padx=5,
                                                      pady=3)

        # ── Ignored Abilities ──
        self.ignore_frame = ttk.LabelFrame(self.inner,
                                            text="Ignored Abilities (one per line)")
        self.ignore_frame.pack(fill=tk.X, padx=5, pady=5)

        self.ignore_text = tk.Text(self.ignore_frame, height=5, width=60,
                                    font=("Consolas", 11))
        self.ignore_text.pack(fill=tk.X, padx=5, pady=5)

    # ── Row builders ──

    def _make_method_combo(self, parent, current="binary_fail"):
        picker = MethodPicker(parent, METHOD_LIST, SCORING_METHODS, width=15)
        picker.set(current)
        return picker

    def _add_mech_row(self, name="", weight=100, method="binary_fail",
                      display="", fix="", role=""):
        r = self.mech_next_row
        self.mech_next_row += 1

        e_name = self._make_ability_combo(self.mech_grid, name, width=18)
        e_name.grid(row=r, column=0, padx=2, pady=1, sticky=tk.W)

        e_weight = tk.Spinbox(self.mech_grid, from_=0, to=150, width=5)
        e_weight.delete(0, tk.END)
        e_weight.insert(0, str(weight))
        e_weight.grid(row=r, column=1, padx=2, pady=1, sticky=tk.W)

        c_method = self._make_method_combo(self.mech_grid, method)
        c_method.grid(row=r, column=2, padx=2, pady=1, sticky=tk.W)

        e_display = ttk.Entry(self.mech_grid, width=22)
        e_display.insert(0, display)
        e_display.grid(row=r, column=3, padx=2, pady=1, sticky=tk.W)

        e_fix = ttk.Entry(self.mech_grid, width=28)
        e_fix.insert(0, fix)
        e_fix.grid(row=r, column=4, padx=2, pady=1, sticky=tk.W)

        c_role = ttk.Combobox(self.mech_grid, values=ROLE_FILTERS,
                              state="readonly", width=10)
        c_role.set(role)
        c_role.grid(row=r, column=5, padx=2, pady=1, sticky=tk.W)

        widgets = {"name": e_name, "weight": e_weight,
                   "method": c_method, "display": e_display,
                   "fix": e_fix, "role": c_role, "_grid_row": r}

        btn = ttk.Button(self.mech_grid, text="X", width=2,
                         command=lambda w=widgets: self._remove_mech_row(w))
        btn.grid(row=r, column=6, padx=2, pady=1)
        widgets["remove"] = btn

        # When an ability is picked from dropdown, remove it from ignored
        e_name._select_callback = self._remove_from_ignored

        self.mech_rows.append(widgets)

    def _add_tank_row(self, debuff="", method="binary", max_safe=1,
                      display="", fix=""):
        r = self.tank_next_row
        self.tank_next_row += 1

        e_debuff = self._make_ability_combo(self.tank_grid, debuff, width=18)
        e_debuff.grid(row=r, column=0, padx=2, pady=1, sticky=tk.W)

        c_method = MethodPicker(self.tank_grid, TANK_METHODS,
                                TANK_METHOD_DESCS, width=8)
        c_method.set(method)
        c_method.grid(row=r, column=1, padx=2, pady=1, sticky=tk.W)

        e_max = ttk.Entry(self.tank_grid, width=12)
        e_max.insert(0, str(max_safe))
        e_max.grid(row=r, column=2, padx=2, pady=1, sticky=tk.W)

        e_display = ttk.Entry(self.tank_grid, width=22)
        e_display.insert(0, display)
        e_display.grid(row=r, column=3, padx=2, pady=1, sticky=tk.W)

        e_fix = ttk.Entry(self.tank_grid, width=30)
        e_fix.insert(0, fix)
        e_fix.grid(row=r, column=4, padx=2, pady=1, sticky=tk.W)

        widgets = {"debuff": e_debuff, "method": c_method,
                   "max_safe": e_max, "display": e_display,
                   "fix": e_fix, "_grid_row": r}

        btn = ttk.Button(self.tank_grid, text="X", width=2,
                         command=lambda w=widgets: self._remove_grid_row(
                             w, self.tank_rows, self.tank_grid))
        btn.grid(row=r, column=5, padx=2, pady=1)
        widgets["remove"] = btn

        self.tank_rows.append(widgets)

    def _add_target_row(self, name="", display="", fix=""):
        r = self.target_next_row
        self.target_next_row += 1

        e_name = self._make_ability_combo(self.target_grid, name, width=20)
        e_name.grid(row=r, column=0, padx=2, pady=1, sticky=tk.W)

        e_display = ttk.Entry(self.target_grid, width=28)
        e_display.insert(0, display)
        e_display.grid(row=r, column=1, padx=2, pady=1, sticky=tk.W)

        e_fix = ttk.Entry(self.target_grid, width=35)
        e_fix.insert(0, fix)
        e_fix.grid(row=r, column=2, padx=2, pady=1, sticky=tk.W)

        widgets = {"name": e_name, "display": e_display,
                   "fix": e_fix, "_grid_row": r}

        btn = ttk.Button(self.target_grid, text="X", width=2,
                         command=lambda w=widgets: self._remove_grid_row(
                             w, self.target_rows, self.target_grid))
        btn.grid(row=r, column=3, padx=2, pady=1)
        widgets["remove"] = btn

        self.target_rows.append(widgets)

    def _add_bonus_row(self, name="", method="bonus", display="", fix="",
                       role=""):
        r = self.bonus_next_row
        self.bonus_next_row += 1

        e_name = self._make_ability_combo(self.bonus_grid, name, width=18)
        e_name.grid(row=r, column=0, padx=2, pady=1, sticky=tk.W)

        c_method = self._make_method_combo(self.bonus_grid, method)
        c_method.grid(row=r, column=1, padx=2, pady=1, sticky=tk.W)

        e_display = ttk.Entry(self.bonus_grid, width=22)
        e_display.insert(0, display)
        e_display.grid(row=r, column=2, padx=2, pady=1, sticky=tk.W)

        e_fix = ttk.Entry(self.bonus_grid, width=28)
        e_fix.insert(0, fix)
        e_fix.grid(row=r, column=3, padx=2, pady=1, sticky=tk.W)

        c_role = ttk.Combobox(self.bonus_grid, values=ROLE_FILTERS,
                              state="readonly", width=10)
        c_role.set(role)
        c_role.grid(row=r, column=4, padx=2, pady=1, sticky=tk.W)

        widgets = {"name": e_name, "method": c_method,
                   "display": e_display, "fix": e_fix,
                   "role": c_role, "_grid_row": r}

        btn = ttk.Button(self.bonus_grid, text="X", width=2,
                         command=lambda w=widgets: self._remove_grid_row(
                             w, self.bonus_rows, self.bonus_grid))
        btn.grid(row=r, column=5, padx=2, pady=1)
        widgets["remove"] = btn

        self.bonus_rows.append(widgets)

    def _remove_grid_row(self, widgets, row_list, grid_frame):
        """Remove a row from a grid layout."""
        r = widgets.get("_grid_row")
        if r is not None:
            for w in grid_frame.grid_slaves(row=r):
                w.destroy()
        if widgets in row_list:
            row_list.remove(widgets)

    def _remove_mech_row(self, widgets):
        """Remove a mechanic row and add its ability to the ignored list."""
        name = widgets["name"].get().strip()
        self._remove_grid_row(widgets, self.mech_rows, self.mech_grid)
        if name:
            self._add_to_ignored(name)

    def _add_to_ignored(self, ability_name):
        """Add an ability to the ignored text box if not already there."""
        raw = self.ignore_text.get("1.0", tk.END).strip()
        existing = [line.strip() for line in raw.split("\n") if line.strip()]
        if ability_name not in existing:
            existing.append(ability_name)
            self.ignore_text.delete("1.0", tk.END)
            self.ignore_text.insert("1.0", "\n".join(existing))

    def _remove_from_ignored(self, ability_name):
        """Remove an ability from the ignored text box."""
        raw = self.ignore_text.get("1.0", tk.END).strip()
        lines = [line.strip() for line in raw.split("\n") if line.strip()]
        if ability_name in lines:
            lines.remove(ability_name)
            self.ignore_text.delete("1.0", tk.END)
            if lines:
                self.ignore_text.insert("1.0", "\n".join(lines))

    # ── Boss management ──

    def _add_boss(self):
        name = simpledialog.askstring("Add Boss", "Boss name:",
                                       parent=self)
        if not name or not name.strip():
            return
        name = name.strip()
        if name in self.boss_data:
            messagebox.showwarning("Exists", f"{name} already exists.")
            return
        self.boss_data[name] = {
            "encounter_id": "",
            "short_name": _auto_short_name(name),
            "mechanics": {},
            "tank_swap_rules": [],
            "target_swap": {},
            "bonus_mechanics": {},
            "ignored": []
        }
        self._refresh_boss_combo()
        self.boss_combo.set(name)
        self._on_boss_select()

    def _remove_boss(self):
        name = self.boss_combo.get()
        if not name:
            return
        if messagebox.askyesno("Confirm",
                               f"Remove boss \"{name}\" and all its data?"):
            del self.boss_data[name]
            self._refresh_boss_combo()
            self._clear_all_rows()
            if self.boss_data:
                first = list(self.boss_data.keys())[0]
                self.boss_combo.set(first)
                self._on_boss_select()

    def _refresh_boss_combo(self):
        names = list(self.boss_data.keys())
        self.boss_combo["values"] = names

    # ── Probe ──

    def _run_probe(self):
        """Launch WCL probe with a live output popup window."""
        if not os.path.isfile(WCL_PROBE):
            probe_name = "wcl_probe.exe" if IS_FROZEN else "wcl_probe.py"
            messagebox.showerror("Error",
                                 f"{probe_name} not found in:\n{SCRIPT_DIR}")
            return

        # Get button/status from Config tab (where probe UI now lives)
        cfg_tab = getattr(self, "_config_tab_ref", None)
        if cfg_tab and hasattr(cfg_tab, "btn_probe"):
            cfg_tab.btn_probe.configure(state=tk.DISABLED)
            cfg_tab.probe_status.configure(text="Probing WCL...",
                                           foreground="blue")
        self._probe_running = True

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        debug_log(f"\n{'='*50}\n  Starting: WCL Probe  ({timestamp})\n{'='*50}\n")

        # ── Build popup window ──
        self.probe_win = pw = tk.Toplevel(self)
        pw.title("WCL Probe — Scanning")
        pw.geometry("850x500")
        pw.resizable(True, True)

        warn = tk.Label(
            pw,
            text=("\u26a0  Do not close the launcher while the probe is "
                  "running.  This window will update live."),
            fg="red", font=("Segoe UI", 11, "bold"),
            wraplength=800, justify=tk.LEFT)
        warn.pack(padx=10, pady=(8, 4))

        self.probe_text = scrolledtext.ScrolledText(
            pw, wrap=tk.WORD, font=("Consolas", 11),
            state=tk.DISABLED, bg="#1e1e1e", fg="#cccccc",
            insertbackground="#cccccc")
        self.probe_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        self.probe_close_btn = ttk.Button(
            pw, text="Close", command=self._close_probe_win,
            state=tk.DISABLED)
        self.probe_close_btn.pack(pady=(4, 8))

        pw.protocol("WM_DELETE_WINDOW", self._on_probe_win_close)

        # ── Launch background thread ──
        def task():
            if IS_FROZEN:
                cmd = [WCL_PROBE]
            else:
                cmd = [sys.executable, "-u", WCL_PROBE]

            kwargs = {}
            if sys.platform == "win32":
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            try:
                env = os.environ.copy()
                env["PYTHONIOENCODING"] = "utf-8"
                env["PYTHONUNBUFFERED"] = "1"
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, encoding="utf-8", errors="replace",
                    cwd=SCRIPT_DIR, bufsize=1,
                    env=env, **kwargs)
                for line in proc.stdout:
                    self.after(0, self._probe_append, line)
                proc.wait()
                success = proc.returncode == 0
                if not success:
                    self.after(0, self._probe_append,
                               f"\n*** Probe exited with code "
                               f"{proc.returncode} ***\n")
                self.after(0, self._on_probe_done, success)
            except Exception as e:
                self.after(0, self._probe_append,
                           f"\n*** ERROR: {e} ***\n")
                self.after(0, self._on_probe_done, False)

        threading.Thread(target=task, daemon=True).start()

    def _probe_append(self, text):
        """Append text to the probe output window and debug log."""
        debug_log(text)
        if not hasattr(self, "probe_text"):
            return
        try:
            if not self.probe_text.winfo_exists():
                return
        except tk.TclError:
            return
        self.probe_text.configure(state=tk.NORMAL)
        self.probe_text.insert(tk.END, text)
        self.probe_text.see(tk.END)
        self.probe_text.configure(state=tk.DISABLED)

    def _on_probe_win_close(self):
        """Intercept window X button while probe is running."""
        if self._probe_running:
            messagebox.showwarning(
                "Probe Running",
                "The WCL probe is still running.\n\n"
                "Please wait for it to finish before closing.",
                parent=self.probe_win)
        else:
            self._close_probe_win()

    def _close_probe_win(self):
        if hasattr(self, "probe_win"):
            try:
                if self.probe_win.winfo_exists():
                    self.probe_win.destroy()
            except tk.TclError:
                pass

    def _on_probe_done(self, success):
        self._probe_running = False

        cfg_tab = getattr(self, "_config_tab_ref", None)
        if cfg_tab and hasattr(cfg_tab, "btn_probe"):
            cfg_tab.btn_probe.configure(state=tk.NORMAL)

        try:
            if hasattr(self, "probe_close_btn") and \
               self.probe_close_btn.winfo_exists():
                self.probe_close_btn.configure(state=tk.NORMAL)
        except tk.TclError:
            pass

        try:
            if hasattr(self, "probe_win") and self.probe_win.winfo_exists():
                title = ("WCL Probe — Complete" if success
                         else "WCL Probe — Failed")
                self.probe_win.title(title)
        except tk.TclError:
            pass

        if success:
            self._load_probe_cache()
            # Reload mechanic_rulesets.json — probe may have added new bosses
            self._load_all()
            zone_name = self.probe_cache.get("zone", {}).get("name", "")
            n_bosses = len(self.probe_cache.get("bosses", {}))
            status = (f"Cache: {zone_name} ({n_bosses} bosses) — "
                      f"{self.probe_cache.get('probe_date', '')}")
            if cfg_tab and hasattr(cfg_tab, "probe_status"):
                cfg_tab.probe_status.configure(text=status, foreground="green")
        else:
            if cfg_tab and hasattr(cfg_tab, "probe_status"):
                cfg_tab.probe_status.configure(
                    text="Probe failed — see output window",
                    foreground="red")

    # ── Load / display ──

    def _load_all(self):
        data = load_json(MECHANIC_FILE, {})
        self.boss_data = data.get("bosses", {})
        self._refresh_boss_combo()
        if self.boss_data:
            first = list(self.boss_data.keys())[0]
            self.boss_combo.set(first)
            self._on_boss_select()

    def _clear_all_rows(self):
        # Destroy all grid widgets except headers (row 0)
        for grid in (self.mech_grid, self.tank_grid,
                     self.target_grid, self.bonus_grid):
            for w in grid.grid_slaves():
                if int(w.grid_info()["row"]) > 0:
                    w.destroy()
        self.mech_rows.clear()
        self.tank_rows.clear()
        self.target_rows.clear()
        self.bonus_rows.clear()
        self.mech_next_row = 1
        self.tank_next_row = 1
        self.target_next_row = 1
        self.bonus_next_row = 1
        self.ignore_text.delete("1.0", tk.END)
        self.encounter_id_entry.delete(0, tk.END)

    def _on_boss_select(self, event=None):
        # Save current boss data before switching
        if self.current_boss and self.current_boss in self.boss_data:
            self._collect_current_boss()

        name = self.boss_combo.get()
        if not name or name not in self.boss_data:
            return
        self.current_boss = name
        boss = self.boss_data[name]

        self._clear_all_rows()

        # Encounter ID
        self.encounter_id_entry.insert(0, str(boss.get("encounter_id", "")))

        # Mechanics
        for ability, info in boss.get("mechanics", {}).items():
            self._add_mech_row(
                name=ability,
                weight=int(info.get("weight", 1.0) * 100),
                method=info.get("method", "binary_fail"),
                display=info.get("display", ""),
                fix=info.get("fix", ""),
                role=info.get("role_filter", ""),
            )

        # Tank swaps
        for rule in boss.get("tank_swap_rules", []):
            ms = rule.get("max_safe", rule.get("ratio_threshold", 1))
            self._add_tank_row(
                debuff=rule.get("debuff", ""),
                method=rule.get("method", "binary"),
                max_safe=ms,
                display=rule.get("display", ""),
                fix=rule.get("fix", ""),
            )

        # Target swaps
        for add_name, info in boss.get("target_swap", {}).items():
            self._add_target_row(
                name=add_name,
                display=info.get("display", ""),
                fix=info.get("fix", ""),
            )

        # Bonus mechanics
        for ability, info in boss.get("bonus_mechanics", {}).items():
            self._add_bonus_row(
                name=ability,
                method=info.get("method", "bonus"),
                display=info.get("display", ""),
                fix=info.get("fix", ""),
                role=info.get("role_filter", ""),
            )

        # Ignored
        ignored = boss.get("ignored", [])
        self.ignore_text.insert("1.0", "\n".join(ignored))

    def _collect_current_boss(self):
        """Read all widgets back into self.boss_data for current boss."""
        if not self.current_boss:
            return

        # Start from existing data to preserve fields we don't edit (e.g. short_name)
        boss = dict(self.boss_data.get(self.current_boss, {}))

        # Encounter IDs (supports comma-separated heroic,mythic)
        eid = self.encounter_id_entry.get().strip()
        boss["encounter_id"] = eid

        # Mechanics
        mechs = {}
        # Preserve existing fields not shown in UI (spell_ids, auto_suggested)
        old_mechs = self.boss_data.get(self.current_boss, {}).get(
            "mechanics", {})
        for row in self.mech_rows:
            ability = row["name"].get().strip()
            if not ability:
                continue
            entry = {
                "method": row["method"].get(),
                "display": row["display"].get().strip(),
                "fix": row["fix"].get().strip(),
            }
            try:
                w = int(row["weight"].get())
                if w != 100:
                    entry["weight"] = w / 100.0
            except ValueError:
                pass
            role = row["role"].get().strip()
            if role:
                entry["role_filter"] = role
            # Preserve spell_ids from probe
            old_entry = old_mechs.get(ability, {})
            if old_entry.get("spell_ids"):
                entry["spell_ids"] = old_entry["spell_ids"]
            mechs[ability] = entry
        boss["mechanics"] = mechs

        # Tank swaps
        tanks = []
        for row in self.tank_rows:
            debuff = row["debuff"].get().strip()
            if not debuff:
                continue
            method = row["method"].get()
            entry = {
                "debuff": debuff,
                "method": method,
                "display": row["display"].get().strip(),
                "fix": row["fix"].get().strip(),
            }
            ms_str = row["max_safe"].get().strip()
            if method == "ratio":
                try:
                    entry["ratio_threshold"] = float(ms_str)
                except ValueError:
                    entry["ratio_threshold"] = 1.5
            else:
                try:
                    entry["max_safe"] = int(ms_str)
                except ValueError:
                    entry["max_safe"] = 1
            tanks.append(entry)
        boss["tank_swap_rules"] = tanks

        # Target swaps
        targets = {}
        for row in self.target_rows:
            add_name = row["name"].get().strip()
            if not add_name:
                continue
            targets[add_name] = {
                "display": row["display"].get().strip(),
                "fix": row["fix"].get().strip(),
            }
        boss["target_swap"] = targets

        # Bonus mechanics
        bonuses = {}
        for row in self.bonus_rows:
            ability = row["name"].get().strip()
            if not ability:
                continue
            entry = {
                "method": row["method"].get(),
                "display": row["display"].get().strip(),
                "fix": row["fix"].get().strip(),
            }
            role = row["role"].get().strip()
            if role:
                entry["role_filter"] = role
            bonuses[ability] = entry
        boss["bonus_mechanics"] = bonuses

        # Ignored
        raw = self.ignore_text.get("1.0", tk.END).strip()
        boss["ignored"] = [line.strip() for line in raw.splitlines()
                           if line.strip()]

        self.boss_data[self.current_boss] = boss

    # ── Save ──

    def _save(self):
        # Collect current boss first
        self._collect_current_boss()

        # Load full file, replace only bosses section
        data = load_json(MECHANIC_FILE, {})
        data["bosses"] = self.boss_data

        save_json(MECHANIC_FILE, data)

        self.save_label.configure(text="Saved!", foreground="green")
        self.after(2000, lambda: self.save_label.configure(text=""))


# ═══════════════════════════════════════════════════════════════
#  Tab: Instructions (landing tab)
# ═══════════════════════════════════════════════════════════════

class InstructionsTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)

        canvas = tk.Canvas(self)
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=canvas.yview)
        self.inner = ttk.Frame(canvas)

        self.inner.bind("<Configure>",
                        lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        self.canvas_window = canvas.create_window((0, 0), window=self.inner,
                                                   anchor=tk.NW)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(
                        self.canvas_window,
                        width=max(e.width, self.inner.winfo_reqwidth())))

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.canvas = canvas

        self._build_content()

    def _build_content(self):
        f = self.inner

        # Title
        tk.Label(f, text="Raid Tools Launcher — Complete Guide",
                 font=("Segoe UI", 16, "bold")).grid(row=0, column=0, padx=15, pady=(15, 5), sticky=tk.W)
        tk.Label(f, text="Work through the tabs left-to-right on first setup. After that, only Run Scripts is needed weekly.",
                 font=("Segoe UI", 11, "italic"), foreground="#555555"
                 ).grid(row=1, column=0, padx=15, pady=(0, 10), sticky=tk.W)

        sections = [
            # ── OVERVIEW ──
            ("Getting Started — First-Time Setup", (
                "The workflow has five steps on first run. After initial setup, "
                "only Step 5 (Run Scripts) is needed on a regular basis.\n\n"
                "   1.  Config Tab  →  API keys, guild identity, raid zone, dates, consumables.\n"
                "   2.  Probe WCL  →  Scans your logs and auto-populates the Mechanics tab.\n"
                "   3.  Score Weights  →  Adjust how the composite score is calculated.\n"
                "   4.  Mechanics  →  Review and customize scoring rules per boss.\n"
                "   5.  Roster  →  Assign characters to players after first data pull.\n"
                "   6.  Run Scripts  →  Pull data from WCL/Blizzard and generate the report.\n\n"
                "The final output is a formatted .xlsx spreadsheet saved to your Output Directory."
            )),

            # ── CONFIG TAB ──
            ("Config Tab — API Keys", (
                "You need two sets of API credentials:\n\n"
                "Blizzard API:  Create a client at develop.battle.net. You need a Client ID and "
                "Client Secret. These are used to pull character profiles, gear, enchants, "
                "and raid progression directly from Blizzard's armory.\n\n"
                "Warcraft Logs API:  Create a client at warcraftlogs.com/api/clients. "
                "You need a Client ID and Client Secret. These are used to pull fight data, "
                "rankings, damage taken, deaths, and mechanic events.\n\n"
                "Both keys are saved in config.json. Use the 'Get API Key' buttons to open "
                "the signup pages in your browser."
            )),
            ("Config Tab — Guild / Team Identity", (
                "Guild Name:  Your guild name exactly as it appears on WCL (e.g. 'Educated').\n\n"
                "Team Name:  Your raid team name (e.g. 'Team Detention'). This is used in "
                "the output filename and report headers.\n\n"
                "Server:  The realm slug (e.g. 'stormrage', 'area-52', 'mal-ganis'). "
                "Lowercase, hyphens for spaces/apostrophes.\n\n"
                "Region:  US or EU.\n\n"
                "WCL Guild IDs:  Find these from your guild page URL on WCL. Go to "
                "warcraftlogs.com, navigate to your guild, and look at the URL — it contains "
                "the numeric guild ID. If your guild has raid teams, each team has its own ID "
                "separate from the parent guild ID.\n\n"
                "   •  Team Guild ID:  The ID for your specific raid team.\n"
                "   •  Parent Guild ID:  The ID for the overall guild.\n\n"
                "If your guild has no teams, put the same ID in both fields."
            )),
            ("Config Tab — Raid Settings & Dates", (
                "Raid Zones:  Enter the zone name exactly as it appears on WCL. Enable "
                "the checkbox for each active raid. The zone ID is looked up automatically.\n\n"
                "Start Date:  The first date to pull reports from (YYYY-MM-DD format). "
                "This is typically the start of the current raid tier or season.\n\n"
                "End Date:  Optional. Leave blank to pull through today. Set this if you "
                "want to analyze a specific date range (e.g. a single month).\n\n"
                "Patch Date:  Fallback if Start Date is empty. Typically the patch release date.\n\n"
                "Partition Override:  Leave blank for auto-detection. Only set this if WCL's "
                "partition numbering doesn't match your expectations. Most users never need this."
            )),
            ("Config Tab — Consumables & Output", (
                "Consumable Spell IDs:  These are the WCL ability IDs for potions and "
                "healthstones that get tracked in the report. Defaults cover standard raid "
                "consumables. Use the 'Find Spell IDs' button to search WCL if you need "
                "to add tier-specific or new consumables.\n\n"
                "Output Directory:  Where the final spreadsheet is saved. A Google Drive "
                "sync folder is recommended — the spreadsheets are formatted to look good "
                "in Google Sheets.\n\n"
                "After filling in the Config tab, click Save Config, then click "
                "'Run WCL Probe' at the bottom to scan your raid logs."
            )),
            ("Config Tab — WCL Probe", (
                "The Probe scans your raid logs and auto-detects:\n\n"
                "   •  Every damage ability players are taking per boss.\n"
                "   •  Target-swappable adds (enemies damaged by players).\n"
                "   •  Tank swap debuffs.\n"
                "   •  Interruptible casts.\n\n"
                "It populates the Mechanics tab with suggested scoring methods. You only "
                "need to re-probe when a new raid tier releases or new bosses are added. "
                "It does NOT need to run every week.\n\n"
                "Your manual edits on the Mechanics tab are preserved across future probes — "
                "only genuinely new abilities get auto-suggested. Abilities you've already "
                "reviewed won't be overwritten."
            )),

            # ── SCORE WEIGHTS ──
            ("Score Weights Tab", (
                "The composite score for each player is a weighted average of four components:\n\n"
                "   •  Mechanics (default 40%)  —  How well the player handles boss mechanics.\n"
                "      Graded on pass/fail checks: did they stand in fire, soak the orb,\n"
                "      swap to adds, etc. Higher weight = mechanics matter more.\n\n"
                "   •  Deaths (default 35%)  —  Fewer deaths = higher score. Dying is one of\n"
                "      the most impactful things a raider can do wrong, so this is weighted\n"
                "      heavily by default.\n\n"
                "   •  Parse Performance (default 20%)  —  WCL parse percentile (DPS for\n"
                "      damage dealers, HPS for healers). This reflects raw throughput\n"
                "      compared to the global population.\n\n"
                "   •  Consumables (default 5%)  —  Did the player use pots and healthstones?\n"
                "      Low weight because it's binary (used or didn't), but still contributes.\n\n"
                "Weights must total 100%. Adjust based on what your raid team values most.\n\n"
                "Grade Thresholds define the minimum composite score for each letter grade:\n"
                "   •  A = 90+  (top performers, minimal mistakes)\n"
                "   •  B = 80+  (solid, occasional errors)\n"
                "   •  C = 70+  (average, room for improvement)\n"
                "   •  D = 60+  (below average, frequent issues)\n"
                "   •  F = below 60  (significant problems)\n\n"
                "These are fully adjustable to match your team's standards."
            )),

            # ── MECHANICS TAB ──
            ("Mechanics Tab — Overview", (
                "This is where you define HOW each boss ability is scored. The Probe fills "
                "this in automatically, but you should review and adjust. Each boss has "
                "three sections:\n\n"
                "   •  Mechanics  —  Damage-taken abilities (fire patches, beam hits, etc.)\n"
                "   •  Tank Swap Rules  —  Debuff-based tank swap tracking.\n"
                "   •  Bonus Mechanics  —  Positive actions (interrupts, target swaps).\n\n"
                "Select a boss from the dropdown to view and edit its rules. Changes auto-save "
                "when you switch bosses or save the file."
            )),
            ("Mechanics Tab — Scoring Methods Explained", (
                "Each mechanic ability gets a scoring method from the dropdown:\n\n"
                "binary_fail  —  ANY damage taken = FAIL. Use for completely avoidable "
                "abilities like standing in fire, getting hit by a beam, or failing to dodge. "
                "This is the most common method. If a player takes even 1 point of damage "
                "from this ability, they get a penalty.\n\n"
                "relative_fail  —  Damage taken ABOVE the raid median × 1.5 = FAIL. Use for "
                "abilities where some damage is unavoidable (proximity damage, overlap areas) "
                "but excessive damage means bad positioning. Players who take a normal amount "
                "get OK, players who take way more than average get FAIL.\n\n"
                "binary_pass  —  ANY damage taken = PASS (bonus). Use for soak mechanics "
                "where you WANT players to take damage — orb intercepts, soak circles, etc. "
                "Taking 0 damage = FAIL (you didn't soak).\n\n"
                "immune_soak  —  Like binary_pass, but immune classes (Mage, Rogue, Paladin) "
                "who take 0 damage still get PASS since they can immune the soak. Everyone "
                "else must take damage to pass.\n\n"
                "conditional_fail  —  Only scores if a trigger condition is met. Currently "
                "supports 'non_tank_if_damage_exists': if ANYONE in the raid took damage "
                "from this ability, all non-tanks fail. Use for abilities that fire because "
                "someone failed a mechanic (e.g. boss casts Arcane Overflow because pylons "
                "weren't soaked — everyone gets penalized).\n\n"
                "ignore  —  Not scored. Use for abilities that are 100% unavoidable or "
                "irrelevant to player performance.\n\n"
                "target_swap  —  (Bonus Mechanics section) Checks if the player dealt damage "
                "to a specific add/target. Damage > 0 = PASS, 0 = FAIL.\n\n"
                "bonus  —  (Bonus Mechanics section) Special positive action detected."
            )),
            ("Mechanics Tab — Role Filters", (
                "Each mechanic can have a role filter applied:\n\n"
                "   (blank)  —  Applies to everyone.\n"
                "   non_tank  —  Only scored for DPS and healers. Tanks are skipped.\n"
                "   tank_only  —  Only scored for tanks.\n"
                "   healer_only  —  Only scored for healers.\n\n"
                "Use these when a mechanic only applies to certain roles. For example, "
                "a frontal cone that only tanks should be in front of would be 'tank_only', "
                "while a soak that tanks are exempt from would be 'non_tank'."
            )),
            ("Mechanics Tab — Tank Swap Rules", (
                "Tank swaps are scored separately using debuff application data:\n\n"
                "binary  —  The boss applies a debuff with each swing/cast. Tanks must swap "
                "at a certain stack count. Set 'max_safe' to the number of applications "
                "before a swap is required. If the co-tank's applications exceed max_safe, "
                "the tank holding too long gets a FAIL.\n\n"
                "ratio  —  For continuously stacking debuffs where exact swap timing varies. "
                "Compares the distribution of stacks between both tanks. If the ratio exceeds "
                "the threshold (default 2.0), the tank with fewer stacks is penalized for "
                "not taunting soon enough.\n\n"
                "Tank swap tracking requires the debuff ability name to exactly match what "
                "appears in the WCL logs."
            )),
            ("Mechanics Tab — Display Name & Fix Text", (
                "Display Name:  A human-readable label shown in the report (e.g. 'Stood in Fire' "
                "instead of 'Blazing Eruption'). If blank, the raw ability name is used.\n\n"
                "Fix Text:  A short coaching note shown alongside failures in the Character View "
                "mechanic breakdown (e.g. 'Move out of the swirl within 2 seconds'). "
                "This helps players understand what to do differently. Optional but recommended."
            )),
            ("Mechanics Tab — Ignore List", (
                "The text box at the bottom of the Mechanics tab contains ability names that "
                "are intentionally not scored — one per line. This is where purely unavoidable "
                "boss damage goes.\n\n"
                "When the Probe runs, it auto-adds common unavoidable abilities here. "
                "You can manually add abilities to suppress them from future probe suggestions.\n\n"
                "If you accidentally ignore something important, remove it from the list and "
                "add it as a mechanic row above."
            )),

            # ── ROSTER TAB ──
            ("Roster Tab — Player Management", (
                "After your first Raid Pull, every character discovered in the logs is "
                "auto-added to the roster. Initially they appear as unassigned entries "
                "where the player name equals the character name.\n\n"
                "Your job is to organize them:\n\n"
                "   1.  Set the Player Name to the actual person's name/handle.\n"
                "   2.  Set their Main character name.\n"
                "   3.  Add any Alts (comma-separated) so they're tracked under one player.\n"
                "   4.  Exclude guests/pugs/other-team players you don't want tracked.\n\n"
                "Team identity fields (guild, team, server, region) auto-fill from the "
                "Config tab and update whenever Config is saved."
            )),
            ("Roster Tab — Lock Roster", (
                "The Lock Roster checkbox controls two major behaviors:\n\n"
                "UNLOCKED (discovery mode):\n"
                "   •  All characters count toward raid averages.\n"
                "   •  All fights are pulled regardless of who was in them.\n"
                "   •  Blizzard API calls are made for everyone.\n"
                "   •  Use this during initial setup while still identifying your team.\n\n"
                "LOCKED (team mode):\n"
                "   •  Only rostered characters count toward raid averages.\n"
                "   •  Guests/pugs are scored individually but don't skew team metrics.\n"
                "   •  Blizzard API calls are skipped for non-roster players (saves time).\n"
                "   •  Minimum roster player filter activates (set to 8) — fights without\n"
                "      at least 8 of your roster members are excluded from the pull.\n"
                "   •  This is the intended mode for weekly use once your roster is set up.\n\n"
                "Locking automatically sets min_roster_players to 8 in config.json. "
                "Unlocking sets it back to 0."
            )),
            ("Roster Tab — Excluding Characters", (
                "Click the ✖ button on any roster row to exclude that character.\n\n"
                "Excluded characters:\n"
                "   •  Are removed from Summary, Raids, Character View, and Roster sheets.\n"
                "   •  Do NOT appear in the Character View dropdown.\n"
                "   •  Do NOT count toward any raid averages.\n"
                "   •  Will NOT be re-added to the roster on future pulls.\n"
                "   •  DO still appear on Raid Performance IF they were in a fight that night,\n"
                "      but their data doesn't affect the raid's average calculations.\n\n"
                "Use this for pugs, players from other teams, trial players who left, "
                "or anyone you don't want cluttering your report.\n\n"
                "Excluded characters appear in a section below the roster grid with ↩ Restore "
                "buttons if you need to bring someone back."
            )),
            ("Roster Tab — Rebuild Report", (
                "After saving roster changes, a 'Rebuild Report' button appears. This re-runs "
                "the Build Tracker using your existing pulled data (raid_dataframes.xlsx) with "
                "the updated roster assignments.\n\n"
                "Use this when you've reorganized players, locked/unlocked the roster, or "
                "excluded characters — you don't need to re-pull data from WCL just to "
                "update the spreadsheet with roster changes."
            )),

            # ── RUN SCRIPTS TAB ──
            ("Run Scripts Tab — Running Pulls", (
                "Three run modes:\n\n"
                "   •  Run Raid Pull  —  Downloads all fight data from WCL and Blizzard for "
                "      every report in your configured date range. Produces raid_dataframes.xlsx.\n"
                "      This is the data-gathering step. Run weekly or when new logs are uploaded.\n\n"
                "   •  Run Build Tracker  —  Reads raid_dataframes.xlsx, applies your mechanic "
                "      rules, roster assignments, and score weights, then produces the final "
                "      formatted spreadsheet. Output is saved to your Output Directory as:\n"
                "      '{Team Name} [{most recent raid date}].xlsx'\n\n"
                "   •  Run Both (Chained)  —  Runs Raid Pull, then immediately runs Build "
                "      Tracker when the pull completes. This is the most common weekly workflow.\n\n"
                "A popup console window opens showing live progress. Do not close the launcher "
                "while a script is running. Use the Stop button to cancel if needed."
            )),
            ("Run Scripts Tab — Pull Options", (
                "Include parent guild logs:  Enable this when your raid team's WCL ID was "
                "created mid-season. Normally, only logs tagged to your Team Guild ID are "
                "pulled. With this enabled, logs from the Parent Guild ID are also queried "
                "and merged (deduplicated by report code). This catches logs from before "
                "your team had its own ID.\n\n"
                "When to use this:\n"
                "   •  Your team recently got its own WCL page and older logs are under the guild.\n"
                "   •  Different loggers sometimes tag to the guild vs. the team.\n"
                "   •  You're seeing fewer reports than expected.\n\n"
                "When to turn it off:\n"
                "   •  Your team has always had its own ID.\n"
                "   •  You only want logs explicitly tagged to your team.\n"
                "   •  Combined with a locked roster and min_roster_players, this filters\n"
                "      properly even with extra reports — but fewer reports = faster pulls."
            )),
            ("Run Scripts Tab — Timers & Scheduling", (
                "Repeat Timer:  Runs the selected script at a set interval while the launcher "
                "is open. Set hours (1-168) and pick which script to repeat. The timer stops "
                "when you close the app.\n\n"
                "Windows Task Scheduler:  Registers a system task that runs even when the "
                "launcher is closed. Pick the day of week and time. This is ideal for "
                "automatic weekly updates — set it for the morning after raid night.\n\n"
                "Use 'Register Schedule' to create the task and 'Remove Schedule' to delete it."
            )),

            # ── OUTPUT SPREADSHEET ──
            ("The Output Spreadsheet — Sheet Guide", (
                "The final report contains these visible sheets:\n\n"
                "Summary  —  Raid Readiness overview. Shows mains and assigned alts with "
                "gear status (item level, missing enchants, empty sockets, tier count), "
                "raid progression, and a quick readiness check. Excluded characters and "
                "unassigned alts are filtered out.\n\n"
                "Raids  —  Two performance tables: Best Score vs Raid Average and Average "
                "Score vs Raid Average, broken out by boss. Shows composite score, delta "
                "arrows (▲/▼), and per-boss performance. Excluded characters are filtered out.\n\n"
                "Raid Performance  —  Night-by-night breakdown. Use the date dropdown to "
                "select a raid night. Shows boss results (kill/wipe, pulls, parse averages) "
                "and player rankings per boss. Excluded characters DO appear here if they "
                "were in a fight, but don't affect averages.\n\n"
                "Character View  —  Deep dive into individual players. Select a character or "
                "player (👤 prefix) from the dropdown. Shows parse history, death analysis, "
                "attendance, mechanic failures per boss, and performance trends over time.\n\n"
                "Roster  —  The full organized roster with mains, alts, composition breakdown "
                "by class, and a raid buff tracker showing which buffs your team covers."
            )),
            ("The Output Spreadsheet — Hidden Sheets", (
                "Several hidden sheets power the dropdown formulas and charts:\n\n"
                "   •  Scorecard  —  Player rankings by composite score for the latest date.\n"
                "      Used as a data source by other sheets. Hidden but preserved.\n"
                "   •  d_rp_boss / d_rp_player / d_rp_detail  —  Raid Performance data.\n"
                "   •  d_cv_info / d_cv_boss / d_cv_deaths / etc.  —  Character View data.\n"
                "   •  d_scores / d_boss_scores  —  Score calculation data.\n"
                "   •  chart_data / d_chart_dyn  —  Trend chart data.\n\n"
                "These are auto-generated and should not be edited manually."
            )),

            # ── DATA FLOW ──
            ("Understanding the Data Pipeline", (
                "The system works in two stages:\n\n"
                "Stage 1 — Raid Pull (raid_pull.py):\n"
                "   Queries WCL and Blizzard APIs across multiple phases:\n"
                "   Phase 1:  Discover report codes from WCL for your guild/date range.\n"
                "   Phase 2:  Pull fight metadata (boss names, kill/wipe, players in each fight).\n"
                "   Phase 3:  Pull DPS/HPS rankings for every fight.\n"
                "   Phase 4:  Pull deaths, damage taken, abilities, consumables.\n"
                "   Phase 4.1:  Pull mechanic-specific data (interrupts, target swaps, tank debuffs).\n"
                "   Phase 4.5:  Pull DPS/HPS for wipe fights (separate from ranked kills).\n"
                "   Phase 5:  Build unique player list from filtered fights.\n"
                "   Phase 6:  Blizzard API — character profiles, gear, enchants, progression.\n"
                "   Phase 8:  Guild zone rankings from WCL.\n"
                "   Phase 9:  Update roster.json with newly discovered characters.\n"
                "   Output:  raid_dataframes.xlsx (all raw data for Build Tracker).\n\n"
                "Stage 2 — Build Tracker (build_tracker_v4.py):\n"
                "   Reads raid_dataframes.xlsx + roster.json + mech_rulesets.json.\n"
                "   Scores every player on mechanics, deaths, parses, consumables.\n"
                "   Generates the final formatted report spreadsheet.\n\n"
                "The pull supports checkpointing — if it crashes mid-run, restart it and "
                "it resumes from the last checkpoint. Delete raid_pull_resume.pkl to force "
                "a fresh run."
            )),

            # ── TROUBLESHOOTING ──
            ("Troubleshooting", (
                "No reports found:\n"
                "   •  Check that Start Date isn't in the future.\n"
                "   •  Verify your WCL Guild IDs are correct (check the URL on WCL).\n"
                "   •  Try enabling 'Include parent guild logs' if your team ID is new.\n"
                "   •  Make sure the raid zone is enabled and spelled correctly in Config.\n\n"
                "Too many irrelevant fights from other teams:\n"
                "   •  Lock your roster — this activates the min_roster_players filter (8).\n"
                "   •  Only fights with 8+ of your roster members will be processed.\n\n"
                "Characters keep getting re-added after exclusion:\n"
                "   •  Make sure you're saving the roster after excluding characters.\n"
                "   •  Excluded characters are stored in roster.json and checked during pulls.\n\n"
                "Blizzard API returning iLvl:ERR or 0 slots:\n"
                "   •  The character may have a special character in their name that doesn't\n"
                "      URL-encode cleanly, or they may be on a different region.\n"
                "   •  This is cosmetic — scoring still works from WCL data.\n\n"
                "Build Tracker crashes with TypeError:\n"
                "   •  Usually caused by unexpected data types (NaN values in class/spec).\n"
                "   •  Re-pull data to refresh. If persistent, check debug.log.\n\n"
                "Script seems stuck or frozen:\n"
                "   •  Check the popup console — it shows live progress.\n"
                "   •  Large date ranges with many reports take time (especially Phase 6).\n"
                "   •  Blizzard API has rate limits — the script auto-throttles.\n"
                "   •  WCL point usage is logged. Check if you're near the hourly limit."
            )),

            # ── TIPS ──
            ("Tips & Best Practices", (
                "Weekly workflow:  Lock roster → Run Both (Chained) → open the output .xlsx.\n"
                "That's it for routine use. Everything else is one-time setup.\n\n"
                "Roster changes:  After reorganizing players or excluding characters, use "
                "'Rebuild Report' on the Roster tab instead of re-pulling. It's instant.\n\n"
                "New raid tier:  Update the raid zone name in Config, run the Probe to "
                "detect new boss mechanics, review the Mechanics tab, then pull.\n\n"
                "Mid-season roster changes:  New players are auto-discovered on pull. "
                "Just assign them in the Roster tab and rebuild.\n\n"
                "Mechanic tuning:  Start with the Probe's suggestions, then watch the "
                "first report. If an ability is showing too many false fails (unavoidable "
                "damage flagged as binary_fail), switch it to relative_fail or ignore.\n\n"
                "Performance:  A full-season pull (3+ months, 20+ reports) takes 5-15 minutes "
                "depending on fight count. Subsequent pulls with checkpointing are faster.\n\n"
                "Google Sheets:  The output is optimized for Google Sheets. Upload the .xlsx "
                "to Google Drive and open with Sheets for the best dropdown/formatting "
                "experience. Excel also works but some conditional formatting may differ."
            )),
        ]

        row_num = 2
        for title, body in sections:
            tk.Label(f, text=title, font=("Segoe UI", 12, "bold")
                     ).grid(row=row_num, column=0, padx=15, pady=(14, 2), sticky=tk.W)
            row_num += 1
            tk.Label(f, text=body, font=("Segoe UI", 11),
                     wraplength=900, justify=tk.LEFT
                     ).grid(row=row_num, column=0, padx=25, pady=(0, 5), sticky=tk.W)
            row_num += 1


# ═══════════════════════════════════════════════════════════════
#  Main Application
# ═══════════════════════════════════════════════════════════════

class RaidToolsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Raid Tools Launcher")
        self.root.geometry("1200x800")
        self.root.resizable(True, True)

        # Set default font size for all widgets
        import tkinter.font as tkFont
        default_font = tkFont.nametofont("TkDefaultFont")
        default_font.configure(size=11, family="Segoe UI")
        text_font = tkFont.nametofont("TkTextFont")
        text_font.configure(size=11, family="Segoe UI")

        notebook = ttk.Notebook(root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Active tabs
        self.instructions_tab = InstructionsTab(notebook)
        self.run_tab = RunTab(notebook)
        self.config_tab = ConfigTab(notebook)
        self.roster_tab = RosterTab(notebook)
        self.weights_tab = ScoreWeightsTab(notebook)
        self.mechanics_tab = MechanicsTab(notebook)

        notebook.add(self.instructions_tab, text="Instructions")
        notebook.add(self.config_tab, text="Config")
        notebook.add(self.weights_tab, text="Score Weights")
        notebook.add(self.mechanics_tab, text="Mechanics")
        notebook.add(self.roster_tab, text="Roster")
        notebook.add(self.run_tab, text="Run Scripts")

        # Cross-tab wiring
        self.roster_tab._run_tab_ref = self.run_tab
        self.run_tab._roster_tab_ref = self.roster_tab
        self.config_tab._mechanics_tab_ref = self.mechanics_tab
        self.config_tab._roster_tab_ref = self.roster_tab
        self.mechanics_tab._config_tab_ref = self.config_tab

        # ── Global mousewheel handler ──
        # One binding on root handles all scrollable tabs. Walks up from
        # the widget under the mouse to find the nearest Canvas ancestor.
        self._scrollable_canvases = set()
        for tab in (self.instructions_tab, self.config_tab, self.roster_tab,
                     self.mechanics_tab):
            if hasattr(tab, "canvas"):
                self._scrollable_canvases.add(tab.canvas)
            if hasattr(tab, "excluded_canvas"):
                self._scrollable_canvases.add(tab.excluded_canvas)

        def _global_mousewheel(event):
            # Find the widget under the mouse
            try:
                w = root.winfo_containing(event.x_root, event.y_root)
            except (tk.TclError, AttributeError):
                return
            # Walk up parents looking for a registered scrollable canvas
            while w is not None:
                if w in self._scrollable_canvases:
                    w.yview_scroll(int(-1 * (event.delta / 120)), "units")
                    return
                try:
                    w = w.master
                except AttributeError:
                    break

        root.bind_all("<MouseWheel>", _global_mousewheel)


if __name__ == "__main__":
    debug_log(f"\n\n{'#'*60}\n"
              f"  Raid Tools Launcher started: "
              f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
              f"{'#'*60}\n")
    try:
        root = tk.Tk()
        app = RaidToolsApp(root)
        root.mainloop()
    except Exception:
        import traceback
        log_path = os.path.join(SCRIPT_DIR, "launcher_error.log")
        with open(log_path, "w") as f:
            traceback.print_exc(file=f)
        debug_log(f"\nFATAL ERROR:\n{traceback.format_exc()}\n")
