#!/usr/bin/env python3
"""
SQLBAKReader GUI - Graphical front-end for the bakread CLI tool.

Wraps the bakread executable with a tkinter interface for selecting
backup files, configuring extraction options, and viewing live output.
"""

import csv
import os
import sys
import subprocess
import tempfile
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

BAKREAD_EXE = "bakread.exe" if sys.platform == "win32" else "bakread"


def find_bakread():
    """Locate the bakread executable relative to this script or on PATH."""
    script_dir = Path(__file__).resolve().parent

    candidates = [
        script_dir / "build" / "Release" / BAKREAD_EXE,
        script_dir / "build" / "Debug" / BAKREAD_EXE,
        script_dir / "build" / BAKREAD_EXE,
        script_dir / BAKREAD_EXE,
    ]
    for p in candidates:
        if p.is_file():
            return str(p)

    import shutil
    found = shutil.which(BAKREAD_EXE)
    if found:
        return found

    return BAKREAD_EXE


class FilePickerRow(ttk.Frame):
    """A label + entry + browse button for file selection."""

    def __init__(self, parent, label_text, var, mode="open", filetypes=None,
                 width=50, **kw):
        super().__init__(parent, **kw)
        self.var = var
        self.mode = mode
        self.filetypes = filetypes or [("All files", "*.*")]

        lbl = ttk.Label(self, text=label_text, width=22, anchor="e")
        lbl.pack(side="left", padx=(0, 6))

        ent = ttk.Entry(self, textvariable=var, width=width)
        ent.pack(side="left", fill="x", expand=True, padx=(0, 4))

        btn = ttk.Button(self, text="Browse...", width=9, command=self._browse)
        btn.pack(side="left")

    def _browse(self):
        if self.mode == "open":
            path = filedialog.askopenfilename(filetypes=self.filetypes)
        else:
            path = filedialog.asksaveasfilename(
                filetypes=self.filetypes,
                defaultextension=self.filetypes[0][1] if self.filetypes else "",
            )
        if path:
            self.var.set(path)


class LabeledEntry(ttk.Frame):
    """A label + entry pair."""

    def __init__(self, parent, label_text, var, width=50, show="", **kw):
        super().__init__(parent, **kw)
        lbl = ttk.Label(self, text=label_text, width=22, anchor="e")
        lbl.pack(side="left", padx=(0, 6))
        ent = ttk.Entry(self, textvariable=var, width=width, show=show)
        ent.pack(side="left", fill="x", expand=True)


class LabeledCombo(ttk.Frame):
    """A label + combobox pair."""

    def __init__(self, parent, label_text, var, values, width=18, **kw):
        super().__init__(parent, **kw)
        lbl = ttk.Label(self, text=label_text, width=22, anchor="e")
        lbl.pack(side="left", padx=(0, 6))
        cb = ttk.Combobox(self, textvariable=var, values=values,
                          state="readonly", width=width)
        cb.pack(side="left")


class MultiFilePicker(ttk.LabelFrame):
    """A multi-file picker with a listbox and Add/Remove/Clear buttons."""

    def __init__(self, parent, text="Backup files (.bak)", filetypes=None,
                 on_change=None, **kw):
        super().__init__(parent, text=text, **kw)
        self.filetypes = filetypes or [("SQL Backup", "*.bak"), ("All files", "*.*")]
        self._on_change = on_change
        self._paths = []

        inner = ttk.Frame(self)
        inner.pack(fill="both", expand=True, padx=6, pady=(2, 6))

        list_frame = ttk.Frame(inner)
        list_frame.pack(side="left", fill="both", expand=True)

        self._listbox = tk.Listbox(
            list_frame, height=3, selectmode="extended",
            font=("Consolas", 9))
        lb_scroll = ttk.Scrollbar(list_frame, orient="vertical",
                                  command=self._listbox.yview)
        self._listbox.configure(yscrollcommand=lb_scroll.set)
        self._listbox.pack(side="left", fill="both", expand=True)
        lb_scroll.pack(side="left", fill="y")

        btn_frame = ttk.Frame(inner)
        btn_frame.pack(side="left", padx=(6, 0))

        ttk.Button(btn_frame, text="Add Files...", width=12,
                   command=self._add_files).pack(pady=(0, 3))
        ttk.Button(btn_frame, text="Remove", width=12,
                   command=self._remove_selected).pack(pady=(0, 3))
        ttk.Button(btn_frame, text="Clear All", width=12,
                   command=self._clear_all).pack()

    def _add_files(self):
        paths = filedialog.askopenfilenames(filetypes=self.filetypes)
        if paths:
            for p in paths:
                if p not in self._paths:
                    self._paths.append(p)
                    self._listbox.insert("end", p)
            self._fire_change()

    def _remove_selected(self):
        sel = list(self._listbox.curselection())
        if not sel:
            return
        for idx in reversed(sel):
            self._listbox.delete(idx)
            del self._paths[idx]
        self._fire_change()

    def _clear_all(self):
        self._listbox.delete(0, "end")
        self._paths.clear()
        self._fire_change()

    def _fire_change(self):
        if self._on_change:
            self._on_change()

    def get_paths(self):
        return list(self._paths)

    def has_files(self):
        return len(self._paths) > 0


class CollapsibleSection(ttk.LabelFrame):
    """A label-frame whose body can be toggled visible/hidden."""

    def __init__(self, parent, text, collapsed=False, **kw):
        super().__init__(parent, text=text, **kw)
        self._body = ttk.Frame(self)
        self._collapsed = collapsed
        if not collapsed:
            self._body.pack(fill="x", padx=6, pady=(2, 6))

    @property
    def body(self):
        return self._body

    def toggle(self):
        self._collapsed = not self._collapsed
        if self._collapsed:
            self._body.pack_forget()
        else:
            self._body.pack(fill="x", padx=6, pady=(2, 6))


class BakreadGUI(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("SQLBAKReader")
        self.geometry("900x870")
        self.minsize(750, 700)
        self._process = None
        self._tmp_csv = None

        self._apply_theme()
        self._create_variables()
        self._build_ui()
        self._update_command_preview()

    # ── theming ──────────────────────────────────────────────────────

    def _apply_theme(self):
        style = ttk.Style(self)
        available = style.theme_names()
        for name in ("vista", "winnative", "clam", "alt"):
            if name in available:
                style.theme_use(name)
                break
        style.configure("Run.TButton", font=("Segoe UI", 10, "bold"))
        style.configure("Stop.TButton", font=("Segoe UI", 10))

    # ── tk variables ─────────────────────────────────────────────────

    def _create_variables(self):
        self.v_table = tk.StringVar()
        self.v_out = tk.StringVar()
        self.v_format = tk.StringVar(value="csv")
        self.v_mode = tk.StringVar(value="auto")

        self.v_backupset = tk.StringVar()
        self.v_columns = tk.StringVar()
        self.v_where = tk.StringVar()
        self.v_max_rows = tk.StringVar()
        self.v_delimiter = tk.StringVar(value=",")
        self.v_allocation_hint = tk.StringVar()

        self.v_verbose = tk.BooleanVar(value=False)
        self.v_log_file = tk.StringVar()

        self.v_tde_pfx = tk.StringVar()
        self.v_tde_key = tk.StringVar()
        self.v_tde_pw = tk.StringVar()
        self.v_backup_pfx = tk.StringVar()
        self.v_source_srv = tk.StringVar()
        self.v_target_srv = tk.StringVar()
        self.v_master_pw = tk.StringVar()
        self.v_allow_export = tk.BooleanVar(value=False)
        self.v_cleanup = tk.BooleanVar(value=False)

        for v in (self.v_table, self.v_out, self.v_format,
                  self.v_mode, self.v_backupset, self.v_columns,
                  self.v_where, self.v_max_rows, self.v_delimiter,
                  self.v_allocation_hint, self.v_verbose, self.v_log_file,
                  self.v_tde_pfx, self.v_tde_key, self.v_tde_pw, self.v_backup_pfx,
                  self.v_source_srv, self.v_target_srv, self.v_master_pw,
                  self.v_allow_export, self.v_cleanup):
            v.trace_add("write", lambda *_: self._update_command_preview())

    # ── UI construction ──────────────────────────────────────────────

    def _build_ui(self):
        pad = dict(padx=8, pady=3, fill="x")

        # ── Backup files ──────────────────────────────────────────────
        self._bak_picker = MultiFilePicker(
            self, text="Backup Files (.bak) -- add multiple for striped backups",
            filetypes=[("SQL Backup", "*.bak"), ("All files", "*.*")],
            on_change=self._update_command_preview,
        )
        self._bak_picker.pack(padx=10, pady=(10, 4), fill="x")

        # ── Required ─────────────────────────────────────────────────
        sec_req = ttk.LabelFrame(self, text="Required")
        sec_req.pack(padx=10, pady=(0, 4), fill="x")

        LabeledEntry(sec_req, "Table (schema.table):", self.v_table).pack(**pad)

        self._out_picker = FilePickerRow(
            sec_req, "Output file:", self.v_out, mode="save",
            filetypes=[("CSV", "*.csv"), ("Parquet", "*.parquet"),
                       ("JSON Lines", "*.jsonl"), ("All files", "*.*")],
        )
        self._out_picker.pack(**pad)

        fmt_row = ttk.Frame(sec_req)
        fmt_row.pack(**pad)
        LabeledCombo(
            fmt_row, "Format:", self.v_format,
            values=["csv", "parquet", "jsonl"],
        ).pack(side="left")
        LabeledCombo(
            fmt_row, "Mode:", self.v_mode,
            values=["auto", "direct", "restore"],
        ).pack(side="left", padx=(20, 0))

        # ── Filtering ────────────────────────────────────────────────
        sec_filt = CollapsibleSection(self, text="Filtering Options")
        sec_filt.pack(padx=10, pady=4, fill="x")

        body = sec_filt.body
        LabeledEntry(body, "Backup set #:", self.v_backupset).pack(**pad)
        LabeledEntry(body, "Columns (comma-sep):", self.v_columns).pack(**pad)
        LabeledEntry(body, "WHERE clause:", self.v_where).pack(**pad)
        LabeledEntry(body, "Max rows:", self.v_max_rows).pack(**pad)
        LabeledEntry(body, "Delimiter:", self.v_delimiter).pack(**pad)
        FilePickerRow(
            body, "Allocation hint CSV:", self.v_allocation_hint, mode="open",
            filetypes=[("CSV", "*.csv"), ("All files", "*.*")],
        ).pack(**pad)

        # ── Restore Mode Options ───────────────────────────────────
        sec_restore = CollapsibleSection(self, text="Restore Mode Options")
        sec_restore.pack(padx=10, pady=4, fill="x")
        
        rbody = sec_restore.body
        ttk.Label(
            rbody, 
            text="Required when Mode = 'restore'. bakread will temporarily restore the backup to this SQL Server.",
            foreground="gray"
        ).pack(anchor="w", padx=8, pady=(4, 8))
        LabeledEntry(rbody, "Target SQL Server:", self.v_target_srv).pack(**pad)
        
        # ── Logging ──────────────────────────────────────────────────
        log_row = ttk.Frame(self)
        log_row.pack(padx=10, pady=4, fill="x")

        chk = ttk.Checkbutton(log_row, text="Verbose", variable=self.v_verbose)
        chk.pack(side="left", padx=(0, 12))
        FilePickerRow(
            log_row, "Log file:", self.v_log_file, mode="save",
            filetypes=[("Log", "*.log"), ("Text", "*.txt"), ("All", "*.*")],
            width=30,
        ).pack(side="left", fill="x", expand=True)

        # ── TDE / Encryption ────────────────────────────────────────
        sec_tde = CollapsibleSection(
            self, text="TDE / Encryption", collapsed=True)
        sec_tde.pack(padx=10, pady=4, fill="x")
        # toggle button
        toggle_btn = ttk.Button(
            sec_tde, text="Show / Hide",
            command=sec_tde.toggle, width=12)
        toggle_btn.pack(anchor="e", padx=4, pady=2)

        tbody = sec_tde.body
        FilePickerRow(
            tbody, "TDE certificate:", self.v_tde_pfx,
            filetypes=[("Certificate", "*.cer;*.pfx"), ("All", "*.*")],
        ).pack(**pad)
        FilePickerRow(
            tbody, "TDE private key:", self.v_tde_key,
            filetypes=[("Private Key", "*.pvk"), ("All", "*.*")],
        ).pack(**pad)
        LabeledEntry(
            tbody, "TDE key password:", self.v_tde_pw, show="*"
        ).pack(**pad)
        FilePickerRow(
            tbody, "Backup cert PFX:", self.v_backup_pfx,
            filetypes=[("PFX", "*.pfx"), ("All", "*.*")],
        ).pack(**pad)
        LabeledEntry(tbody, "Source server (for cert export):", self.v_source_srv).pack(**pad)
        LabeledEntry(
            tbody, "Master key password:", self.v_master_pw, show="*"
        ).pack(**pad)

        chk_row = ttk.Frame(tbody)
        chk_row.pack(**pad)
        ttk.Checkbutton(
            chk_row, text="Allow key export to disk",
            variable=self.v_allow_export,
        ).pack(side="left", padx=(140, 12))
        ttk.Checkbutton(
            chk_row, text="Cleanup keys after extraction",
            variable=self.v_cleanup,
        ).pack(side="left")

        # ── Command preview ──────────────────────────────────────────
        prev_frame = ttk.LabelFrame(self, text="Command Preview")
        prev_frame.pack(padx=10, pady=4, fill="x")
        self.cmd_preview = tk.Text(
            prev_frame, height=2, wrap="word", state="disabled",
            font=("Consolas", 9), background="#f4f4f4")
        self.cmd_preview.pack(fill="x", padx=6, pady=4)

        # ── Run / Stop / Preview ─────────────────────────────────────
        btn_row = ttk.Frame(self)
        btn_row.pack(padx=10, pady=6, fill="x")

        self.btn_run = ttk.Button(
            btn_row, text="Extract", style="Run.TButton",
            command=self._run)
        self.btn_run.pack(side="left", padx=(0, 8))

        self.btn_preview = ttk.Button(
            btn_row, text="Preview Data", style="Run.TButton",
            command=self._preview)
        self.btn_preview.pack(side="left", padx=(0, 8))

        self.btn_stop = ttk.Button(
            btn_row, text="Stop", style="Stop.TButton",
            command=self._stop, state="disabled")
        self.btn_stop.pack(side="left")

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(btn_row, textvariable=self.status_var).pack(
            side="right", padx=4)

        # ── Tabbed output area ───────────────────────────────────────
        self._notebook = ttk.Notebook(self)
        self._notebook.pack(padx=10, pady=(4, 10), fill="both", expand=True)

        # -- Log tab --
        log_frame = ttk.Frame(self._notebook)
        self._notebook.add(log_frame, text="  Log  ")

        self.output_text = tk.Text(
            log_frame, wrap="word", state="disabled",
            font=("Consolas", 9), background="#1e1e1e", foreground="#dcdcdc",
            insertbackground="#dcdcdc")
        log_scroll = ttk.Scrollbar(log_frame, command=self.output_text.yview)
        self.output_text.configure(yscrollcommand=log_scroll.set)
        log_scroll.pack(side="right", fill="y")
        self.output_text.pack(fill="both", expand=True, padx=(4, 0), pady=4)

        self.output_text.tag_configure("stderr", foreground="#f44747")
        self.output_text.tag_configure("warn", foreground="#cca700")
        self.output_text.tag_configure("info", foreground="#569cd6")

        # -- Data tab --
        data_frame = ttk.Frame(self._notebook)
        self._notebook.add(data_frame, text="  Data  ")

        self._build_data_tab(data_frame)

    # ── data tab ──────────────────────────────────────────────────────

    def _build_data_tab(self, parent):
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", padx=4, pady=(4, 2))

        ttk.Label(toolbar, text="Preview rows:").pack(side="left", padx=(0, 4))
        self.v_preview_rows = tk.StringVar(value="500")
        spin = ttk.Spinbox(
            toolbar, from_=10, to=100000, increment=100,
            textvariable=self.v_preview_rows, width=8)
        spin.pack(side="left", padx=(0, 12))

        ttk.Label(toolbar, text="Search:").pack(side="left", padx=(0, 4))
        self.v_data_search = tk.StringVar()
        self.v_data_search.trace_add("write", lambda *_: self._filter_data_view())
        search_entry = ttk.Entry(toolbar, textvariable=self.v_data_search, width=24)
        search_entry.pack(side="left", padx=(0, 12))

        self.data_row_count_var = tk.StringVar(value="No data loaded")
        ttk.Label(toolbar, textvariable=self.data_row_count_var).pack(side="right", padx=4)

        btn_copy = ttk.Button(toolbar, text="Copy Selection",
                              command=self._copy_selection, width=14)
        btn_copy.pack(side="right", padx=4)

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True, padx=4, pady=(0, 4))

        self.data_tree = ttk.Treeview(
            tree_frame, show="headings", selectmode="extended")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical",
                            command=self.data_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal",
                            command=self.data_tree.xview)
        self.data_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.data_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self._data_columns = []
        self._data_rows = []

    def _populate_data_tree(self, columns, rows):
        self._data_columns = columns
        self._data_rows = rows

        self.data_tree.delete(*self.data_tree.get_children())

        col_ids = [f"c{i}" for i in range(len(columns))]
        self.data_tree["columns"] = col_ids

        for cid, name in zip(col_ids, columns):
            self.data_tree.heading(cid, text=name,
                                   command=lambda c=cid: self._sort_column(c))
            self.data_tree.column(cid, width=120, minwidth=60, stretch=True)

        for row in rows:
            self.data_tree.insert("", "end", values=row)

        self.data_row_count_var.set(f"{len(rows):,} rows x {len(columns)} columns")

    def _filter_data_view(self):
        term = self.v_data_search.get().strip().lower()
        self.data_tree.delete(*self.data_tree.get_children())

        if not term:
            visible = self._data_rows
        else:
            visible = [r for r in self._data_rows
                       if any(term in str(cell).lower() for cell in r)]

        for row in visible:
            self.data_tree.insert("", "end", values=row)

        total = len(self._data_rows)
        shown = len(visible)
        if term:
            self.data_row_count_var.set(
                f"Showing {shown:,} of {total:,} rows ({len(self._data_columns)} columns)")
        else:
            self.data_row_count_var.set(
                f"{total:,} rows x {len(self._data_columns)} columns")

    def _sort_column(self, col):
        items = [(self.data_tree.set(k, col), k)
                 for k in self.data_tree.get_children("")]

        try:
            items.sort(key=lambda t: float(t[0]))
        except (ValueError, TypeError):
            items.sort(key=lambda t: t[0].lower())

        for idx, (_, k) in enumerate(items):
            self.data_tree.move(k, "", idx)

        self.data_tree.heading(
            col, command=lambda: self._sort_column_reverse(col))

    def _sort_column_reverse(self, col):
        items = [(self.data_tree.set(k, col), k)
                 for k in self.data_tree.get_children("")]

        try:
            items.sort(key=lambda t: float(t[0]), reverse=True)
        except (ValueError, TypeError):
            items.sort(key=lambda t: t[0].lower(), reverse=True)

        for idx, (_, k) in enumerate(items):
            self.data_tree.move(k, "", idx)

        self.data_tree.heading(
            col, command=lambda: self._sort_column(col))

    def _copy_selection(self):
        selected = self.data_tree.selection()
        if not selected:
            return
        lines = []
        cols = self.data_tree["columns"]
        lines.append("\t".join(
            self.data_tree.heading(c, "text") for c in cols))
        for item in selected:
            vals = self.data_tree.item(item, "values")
            lines.append("\t".join(str(v) for v in vals))
        self.clipboard_clear()
        self.clipboard_append("\n".join(lines))

    # ── command building ─────────────────────────────────────────────

    def _build_command(self):
        exe = find_bakread()
        cmd = [exe]

        for bak in self._bak_picker.get_paths():
            cmd += ["--bak", bak]

        table = self.v_table.get().strip()
        if table:
            cmd += ["--table", table]

        out = self.v_out.get().strip()
        if out:
            cmd += ["--out", out]

        fmt = self.v_format.get()
        if fmt:
            cmd += ["--format", fmt]

        mode = self.v_mode.get()
        if mode and mode != "auto":
            cmd += ["--mode", mode]

        bs = self.v_backupset.get().strip()
        if bs:
            cmd += ["--backupset", bs]

        cols = self.v_columns.get().strip()
        if cols:
            cmd += ["--columns", cols]

        where = self.v_where.get().strip()
        if where:
            cmd += ["--where", where]

        mr = self.v_max_rows.get().strip()
        if mr:
            cmd += ["--max-rows", mr]

        delim = self.v_delimiter.get()
        if delim and delim != ",":
            cmd += ["--delimiter", delim]

        alloc_hint = self.v_allocation_hint.get().strip()
        if alloc_hint:
            cmd += ["--allocation-hint", alloc_hint]

        if self.v_verbose.get():
            cmd += ["--verbose"]

        log = self.v_log_file.get().strip()
        if log:
            cmd += ["--log", log]

        tpfx = self.v_tde_pfx.get().strip()
        if tpfx:
            cmd += ["--tde-cert-pfx", tpfx]

        tkey = self.v_tde_key.get().strip()
        if tkey:
            cmd += ["--tde-cert-key", tkey]

        tpw = self.v_tde_pw.get().strip()
        if tpw:
            cmd += ["--tde-cert-password", tpw]

        bpfx = self.v_backup_pfx.get().strip()
        if bpfx:
            cmd += ["--backup-cert-pfx", bpfx]

        ss = self.v_source_srv.get().strip()
        if ss:
            cmd += ["--source-server", ss]

        ts = self.v_target_srv.get().strip()
        if ts:
            cmd += ["--target-server", ts]

        mkp = self.v_master_pw.get().strip()
        if mkp:
            cmd += ["--master-key-password", mkp]

        if self.v_allow_export.get():
            cmd += ["--allow-key-export-to-disk"]

        if self.v_cleanup.get():
            cmd += ["--cleanup-keys"]

        return cmd

    def _update_command_preview(self):
        cmd = self._build_command()
        display = subprocess.list2cmdline(cmd)
        self.cmd_preview.configure(state="normal")
        self.cmd_preview.delete("1.0", "end")
        self.cmd_preview.insert("1.0", display)
        self.cmd_preview.configure(state="disabled")

    # ── validation ───────────────────────────────────────────────────

    def _validate(self, require_output=True):
        if not self._bak_picker.has_files():
            messagebox.showwarning("Missing field",
                                   "At least one backup file (.bak) is required.")
            return False
        if not self.v_table.get().strip():
            messagebox.showwarning("Missing field", "Table name is required.")
            return False
        if require_output and not self.v_out.get().strip():
            messagebox.showwarning("Missing field", "Output file path is required.")
            return False
        return True

    # ── execution ────────────────────────────────────────────────────

    def _append_output(self, text, tag=None):
        self.output_text.configure(state="normal")
        if tag:
            self.output_text.insert("end", text, tag)
        else:
            self.output_text.insert("end", text)
        self.output_text.see("end")
        self.output_text.configure(state="disabled")

    def _run(self):
        if not self._validate():
            return

        self._notebook.select(0)  # show Log tab

        self.output_text.configure(state="normal")
        self.output_text.delete("1.0", "end")
        self.output_text.configure(state="disabled")

        cmd = self._build_command()
        self._append_output(
            ">> " + subprocess.list2cmdline(cmd) + "\n\n", "info")

        self.btn_run.configure(state="disabled")
        self.btn_preview.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.status_var.set("Running...")

        thread = threading.Thread(target=self._exec_thread, args=(cmd,),
                                  daemon=True)
        thread.start()

    def _preview(self):
        if not self._validate(require_output=False):
            return

        try:
            max_rows = int(self.v_preview_rows.get())
        except ValueError:
            max_rows = 500

        self._notebook.select(0)  # show Log tab during extraction

        self.output_text.configure(state="normal")
        self.output_text.delete("1.0", "end")
        self.output_text.configure(state="disabled")

        self._tmp_csv = tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, prefix="bakread_preview_")
        tmp_path = self._tmp_csv.name
        self._tmp_csv.close()

        cmd = self._build_command_for_preview(tmp_path, max_rows)

        self._append_output(
            ">> Preview: " + subprocess.list2cmdline(cmd) + "\n\n", "info")

        self.btn_run.configure(state="disabled")
        self.btn_preview.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.status_var.set("Extracting preview...")

        thread = threading.Thread(
            target=self._exec_thread, args=(cmd, tmp_path), daemon=True)
        thread.start()

    def _build_command_for_preview(self, tmp_path, max_rows):
        exe = find_bakread()
        cmd = [exe]

        for bak in self._bak_picker.get_paths():
            cmd += ["--bak", bak]

        table = self.v_table.get().strip()
        if table:
            cmd += ["--table", table]

        cmd += ["--out", tmp_path]
        cmd += ["--format", "csv"]
        cmd += ["--max-rows", str(max_rows)]

        mode = self.v_mode.get()
        if mode and mode != "auto":
            cmd += ["--mode", mode]

        bs = self.v_backupset.get().strip()
        if bs:
            cmd += ["--backupset", bs]

        cols = self.v_columns.get().strip()
        if cols:
            cmd += ["--columns", cols]

        where = self.v_where.get().strip()
        if where:
            cmd += ["--where", where]

        if self.v_verbose.get():
            cmd += ["--verbose"]

        log = self.v_log_file.get().strip()
        if log:
            cmd += ["--log", log]

        tpfx = self.v_tde_pfx.get().strip()
        if tpfx:
            cmd += ["--tde-cert-pfx", tpfx]
        tkey = self.v_tde_key.get().strip()
        if tkey:
            cmd += ["--tde-cert-key", tkey]
        tpw = self.v_tde_pw.get().strip()
        if tpw:
            cmd += ["--tde-cert-password", tpw]
        bpfx = self.v_backup_pfx.get().strip()
        if bpfx:
            cmd += ["--backup-cert-pfx", bpfx]
        ss = self.v_source_srv.get().strip()
        if ss:
            cmd += ["--source-server", ss]
        ts = self.v_target_srv.get().strip()
        if ts:
            cmd += ["--target-server", ts]
        mkp = self.v_master_pw.get().strip()
        if mkp:
            cmd += ["--master-key-password", mkp]
        if self.v_allow_export.get():
            cmd += ["--allow-key-export-to-disk"]
        if self.v_cleanup.get():
            cmd += ["--cleanup-keys"]

        return cmd

    @staticmethod
    def _classify_line(line):
        """Return a text tag based on the log-level prefix in the line."""
        if "[ERROR]" in line or "[FATAL]" in line:
            return "stderr"
        if "[WARN " in line:
            return "warn"
        return None

    def _exec_thread(self, cmd, preview_csv_path=None):
        try:
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                startupinfo=startupinfo,
                bufsize=1,
                universal_newlines=True,
            )

            for line in self._process.stdout:
                tag = self._classify_line(line)
                self.after(0, self._append_output, line, tag)

            self._process.wait()
            rc = self._process.returncode
            if rc == 0:
                if preview_csv_path:
                    self.after(0, self._load_preview_csv, preview_csv_path)
                else:
                    self.after(0, self._on_finish, True,
                               "Extraction completed successfully.")
            else:
                self.after(0, self._on_finish, False,
                           f"Process exited with code {rc}.")
                if preview_csv_path:
                    self._cleanup_tmp(preview_csv_path)

        except FileNotFoundError:
            self.after(0, self._on_finish, False,
                       f"Could not find '{cmd[0]}'. "
                       "Make sure bakread is built and accessible.")
            if preview_csv_path:
                self._cleanup_tmp(preview_csv_path)
        except Exception as e:
            self.after(0, self._on_finish, False, str(e))
            if preview_csv_path:
                self._cleanup_tmp(preview_csv_path)
        finally:
            self._process = None

    def _load_preview_csv(self, path):
        try:
            with open(path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    self._on_finish(False, "CSV file was empty - no data extracted.")
                    return
                rows = [tuple(r) for r in reader]

            self._populate_data_tree(headers, rows)
            self._notebook.select(1)  # switch to Data tab
            self._on_finish(True,
                            f"Preview loaded: {len(rows):,} rows, "
                            f"{len(headers)} columns.")
        except Exception as e:
            self._on_finish(False, f"Failed to parse preview CSV: {e}")
        finally:
            self._cleanup_tmp(path)

    @staticmethod
    def _cleanup_tmp(path):
        try:
            os.unlink(path)
        except OSError:
            pass

    def _on_finish(self, success, message):
        tag = "info" if success else "stderr"
        self._append_output(f"\n{message}\n", tag)
        self.status_var.set("Done" if success else "Failed")
        self.btn_run.configure(state="normal")
        self.btn_preview.configure(state="normal")
        self.btn_stop.configure(state="disabled")

    def _stop(self):
        proc = self._process
        if proc and proc.poll() is None:
            proc.terminate()
            self.status_var.set("Stopping...")


def main():
    app = BakreadGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
