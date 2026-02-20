#!/usr/bin/env python3
"""
SQLBAKReader GUI - Modern Azure Data Studio-inspired interface.

A graphical front-end for the bakread CLI tool with:
- Dark theme similar to Azure Data Studio
- Sidebar navigation
- SQL Server connection dialog with authentication options
- Live output streaming
"""

import csv
import ctypes
import os
import sys
import subprocess
import tempfile
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path


def set_dark_title_bar(window):
    """Set dark title bar on Windows 10/11."""
    if sys.platform != "win32":
        return
    
    try:
        window.update()
        # Get the window handle
        hwnd = ctypes.windll.user32.GetParent(window.winfo_id())
        
        # DWMWA_USE_IMMERSIVE_DARK_MODE = 20 (Windows 10 build 18985+)
        # For older Windows 10, use attribute 19
        DWMWA_USE_IMMERSIVE_DARK_MODE = 20
        
        # Set dark mode
        value = ctypes.c_int(1)
        ctypes.windll.dwmapi.DwmSetWindowAttribute(
            hwnd,
            DWMWA_USE_IMMERSIVE_DARK_MODE,
            ctypes.byref(value),
            ctypes.sizeof(value)
        )
        
        # Force redraw the title bar
        window.withdraw()
        window.deiconify()
    except Exception:
        pass  # Silently fail on older Windows versions

BAKREAD_EXE = "bakread.exe" if sys.platform == "win32" else "bakread"

# Color scheme inspired by Azure Data Studio / VS Code
COLORS = {
    "bg_dark": "#1e1e1e",
    "bg_sidebar": "#252526",
    "bg_panel": "#2d2d2d",
    "bg_input": "#3c3c3c",
    "bg_button": "#0e639c",
    "bg_button_hover": "#1177bb",
    "bg_button_secondary": "#3c3c3c",
    "fg_primary": "#cccccc",
    "fg_secondary": "#858585",
    "fg_accent": "#3794ff",
    "fg_success": "#89d185",
    "fg_error": "#f48771",
    "fg_warning": "#cca700",
    "border": "#454545",
    "selection": "#094771",
    "accent": "#0078d4",  # Azure blue accent
}


def find_bakread():
    """Locate the bakread executable."""
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
    return found if found else BAKREAD_EXE


class ModernEntry(tk.Entry):
    """Styled entry widget."""
    def __init__(self, parent, **kw):
        super().__init__(
            parent,
            bg=COLORS["bg_input"],
            fg=COLORS["fg_primary"],
            insertbackground=COLORS["fg_primary"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["fg_accent"],
            **kw
        )


class ModernButton(tk.Button):
    """Styled button widget."""
    def __init__(self, parent, primary=False, **kw):
        bg = COLORS["bg_button"] if primary else COLORS["bg_button_secondary"]
        hover_bg = COLORS["bg_button_hover"] if primary else COLORS["border"]
        super().__init__(
            parent,
            bg=bg,
            fg=COLORS["fg_primary"],
            activebackground=hover_bg,
            activeforeground=COLORS["fg_primary"],
            relief="flat",
            cursor="hand2",
            padx=12,
            pady=4,
            **kw
        )
        self.default_bg = bg
        self.hover_bg = hover_bg
        self.bind("<Enter>", lambda e: self.config(bg=self.hover_bg))
        self.bind("<Leave>", lambda e: self.config(bg=self.default_bg))


class ModernCombobox(ttk.Combobox):
    """Styled combobox."""
    pass


class ModernDialog(tk.Toplevel):
    """Base class for popup dialogs with standard title bar but dark theme."""
    
    def __init__(self, parent, title_text, width=450, height=400):
        super().__init__(parent)
        self.title(title_text)
        self.configure(bg=COLORS["bg_panel"])
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)
        
        self._dialog_width = width
        self._dialog_height = height
        
        # Content frame for subclasses to add content
        self.content_frame = tk.Frame(self, bg=COLORS["bg_panel"])
        self.content_frame.pack(fill="both", expand=True)
        
        # Center on parent
        self.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() - width) // 2
        y = parent.winfo_y() + (parent.winfo_height() - height) // 2
        self.geometry(f"{width}x{height}+{x}+{y}")
        
        # Handle close
        self.protocol("WM_DELETE_WINDOW", self._on_dialog_close)
        
        # Apply dark title bar
        set_dark_title_bar(self)
    
    def _on_dialog_close(self):
        """Override in subclass if needed."""
        self.destroy()


class ConnectionDialog(ModernDialog):
    """SQL Server connection dialog similar to SSMS/Azure Data Studio."""
    
    def __init__(self, parent, initial_values=None):
        self.result = None
        initial = initial_values or {}
        super().__init__(parent, "Connect to SQL Server", width=450, height=380)
        
        self._create_widgets(initial)
        
        self.bind("<Escape>", lambda e: self._on_cancel())
        self.bind("<Return>", lambda e: self._on_connect())
    
    def _on_dialog_close(self):
        self._on_cancel()
        
    def _create_widgets(self, initial):
        pad = {"padx": 20, "pady": 6}
        cf = self.content_frame
        
        # Server name
        self._add_field(cf, "Server name:", "server", initial.get("server", ""), pad)
        
        # Authentication type
        auth_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        auth_frame.pack(fill="x", **pad)
        
        tk.Label(
            auth_frame, text="Authentication:", width=18, anchor="e",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left", padx=(0, 8))
        
        self.v_auth_type = tk.StringVar(value=initial.get("auth_type", "Windows Authentication"))
        self.auth_combo = ttk.Combobox(
            auth_frame, textvariable=self.v_auth_type,
            values=["Windows Authentication", "SQL Server Authentication"],
            state="readonly", width=28
        )
        self.auth_combo.pack(side="left", fill="x", expand=True)
        self.auth_combo.bind("<<ComboboxSelected>>", self._on_auth_change)
        
        # Username
        self._add_field(cf, "Login:", "username", initial.get("username", ""), pad)
        
        # Password
        self._add_field(cf, "Password:", "password", initial.get("password", ""), pad, show="*")
        
        # Trust server certificate
        self.v_trust_cert = tk.BooleanVar(value=initial.get("trust_cert", True))
        trust_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        trust_frame.pack(fill="x", **pad)
        tk.Label(trust_frame, text="", width=18, bg=COLORS["bg_panel"]).pack(side="left")
        tk.Checkbutton(
            trust_frame, text="Trust server certificate",
            variable=self.v_trust_cert,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"]
        ).pack(side="left")
        
        # Encrypt connection
        self.v_encrypt = tk.BooleanVar(value=initial.get("encrypt", False))
        encrypt_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        encrypt_frame.pack(fill="x", padx=20, pady=2)
        tk.Label(encrypt_frame, text="", width=18, bg=COLORS["bg_panel"]).pack(side="left")
        tk.Checkbutton(
            encrypt_frame, text="Encrypt connection",
            variable=self.v_encrypt,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"]
        ).pack(side="left")
        
        # Buttons
        btn_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        btn_frame.pack(fill="x", padx=20, pady=20, side="bottom")
        
        ModernButton(
            btn_frame, text="Connect", primary=True,
            command=self._on_connect, width=12
        ).pack(side="right", padx=(8, 0))
        
        ModernButton(
            btn_frame, text="Cancel",
            command=self._on_cancel, width=12
        ).pack(side="right")
        
        # Initial state
        self._on_auth_change()
        
    def _add_field(self, parent, label, name, value, pad, show=""):
        frame = tk.Frame(parent, bg=COLORS["bg_panel"])
        frame.pack(fill="x", **pad)
        
        tk.Label(
            frame, text=label, width=18, anchor="e",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left", padx=(0, 8))
        
        var = tk.StringVar(value=value)
        setattr(self, f"v_{name}", var)
        
        entry = ModernEntry(frame, textvariable=var, width=30, show=show)
        entry.pack(side="left", fill="x", expand=True)
        setattr(self, f"e_{name}", entry)
        
    def _on_auth_change(self, event=None):
        is_sql = self.v_auth_type.get() == "SQL Server Authentication"
        state = "normal" if is_sql else "disabled"
        self.e_username.config(state=state)
        self.e_password.config(state=state)
        if not is_sql:
            self.v_username.set("")
            self.v_password.set("")
            
    def _on_connect(self):
        server = self.v_server.get().strip()
        if not server:
            messagebox.showerror("Error", "Server name is required", parent=self)
            return
            
        self.result = {
            "server": server,
            "auth_type": self.v_auth_type.get(),
            "username": self.v_username.get().strip(),
            "password": self.v_password.get(),
            "trust_cert": self.v_trust_cert.get(),
            "encrypt": self.v_encrypt.get(),
        }
        self.destroy()
        
    def _on_cancel(self):
        self.result = None
        self.destroy()


class ExportDatabaseDialog(ModernDialog):
    """Dialog to configure database export destination."""
    
    def __init__(self, parent, source_table=""):
        self.result = None
        self.source_table = source_table
        super().__init__(parent, "Export to Database", width=500, height=480)
        
        self._create_widgets()
        self.bind("<Escape>", lambda e: self._on_cancel())
    
    def _on_dialog_close(self):
        self._on_cancel()
        
    def _create_widgets(self):
        pad = {"padx": 20, "pady": 5}
        cf = self.content_frame
        
        # Destination Server Section
        section1 = tk.Label(
            cf, text="Destination Server",
            bg=COLORS["bg_panel"], fg=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold"), anchor="w"
        )
        section1.pack(fill="x", padx=20, pady=(10, 5))
        
        # Server name
        self._add_field(cf, "Server:", "server", "", pad)
        
        # Authentication
        auth_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        auth_frame.pack(fill="x", **pad)
        
        tk.Label(
            auth_frame, text="Authentication:", width=15, anchor="e",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left", padx=(0, 8))
        
        self.v_auth_type = tk.StringVar(value="Windows Authentication")
        self.auth_combo = ttk.Combobox(
            auth_frame, textvariable=self.v_auth_type,
            values=["Windows Authentication", "SQL Server Authentication"],
            state="readonly", width=30
        )
        self.auth_combo.pack(side="left", fill="x", expand=True)
        self.auth_combo.bind("<<ComboboxSelected>>", self._on_auth_change)
        
        # Username/Password
        self._add_field(cf, "Login:", "username", "", pad)
        self._add_field(cf, "Password:", "password", "", pad, show="*")
        
        # Destination Section
        section2 = tk.Label(
            cf, text="Destination Table",
            bg=COLORS["bg_panel"], fg=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold"), anchor="w"
        )
        section2.pack(fill="x", padx=20, pady=(10, 5))
        
        # Database name
        self._add_field(cf, "Database:", "database", "", pad)
        
        # Table name
        default_table = self.source_table if self.source_table else "dbo.ExportedData"
        self._add_field(cf, "Table:", "table", default_table, pad)
        
        # Options Section
        section3 = tk.Label(
            cf, text="Options",
            bg=COLORS["bg_panel"], fg=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold"), anchor="w"
        )
        section3.pack(fill="x", padx=20, pady=(10, 5))
        
        # Options checkboxes
        opt_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        opt_frame.pack(fill="x", padx=35, pady=5)
        
        self.v_create_table = tk.BooleanVar(value=True)
        tk.Checkbutton(
            opt_frame, text="Create table if not exists",
            variable=self.v_create_table,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"]
        ).pack(anchor="w")
        
        self.v_truncate = tk.BooleanVar(value=False)
        tk.Checkbutton(
            opt_frame, text="Truncate existing data",
            variable=self.v_truncate,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"]
        ).pack(anchor="w")
        
        self.v_batch_size = tk.StringVar(value="1000")
        batch_frame = tk.Frame(opt_frame, bg=COLORS["bg_panel"])
        batch_frame.pack(fill="x", pady=5)
        tk.Label(
            batch_frame, text="Batch size:",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left")
        ModernEntry(batch_frame, textvariable=self.v_batch_size, width=10).pack(side="left", padx=(8, 0))
        
        # Buttons
        btn_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        btn_frame.pack(fill="x", padx=20, pady=15, side="bottom")
        
        ModernButton(btn_frame, text="Cancel", command=self._on_cancel).pack(side="right")
        ModernButton(btn_frame, text="Export", primary=True, command=self._on_export).pack(side="right", padx=(0, 8))
        
        # Initial state
        self._on_auth_change()
        
    def _add_field(self, parent, label, name, default="", pack_opts=None, show=None):
        pack_opts = pack_opts or {}
        frame = tk.Frame(parent, bg=COLORS["bg_panel"])
        frame.pack(fill="x", **pack_opts)
        
        tk.Label(
            frame, text=label, width=15, anchor="e",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left", padx=(0, 8))
        
        var = tk.StringVar(value=default)
        setattr(self, f"v_{name}", var)
        
        entry = ModernEntry(frame, textvariable=var, width=32, show=show)
        entry.pack(side="left", fill="x", expand=True)
        setattr(self, f"e_{name}", entry)
        
    def _on_auth_change(self, event=None):
        is_sql = self.v_auth_type.get() == "SQL Server Authentication"
        state = "normal" if is_sql else "disabled"
        self.e_username.config(state=state)
        self.e_password.config(state=state)
        
    def _on_export(self):
        server = self.v_server.get().strip()
        database = self.v_database.get().strip()
        table = self.v_table.get().strip()
        
        if not server:
            messagebox.showerror("Error", "Server name is required", parent=self)
            return
        if not database:
            messagebox.showerror("Error", "Database name is required", parent=self)
            return
        if not table:
            messagebox.showerror("Error", "Table name is required", parent=self)
            return
            
        self.result = {
            "server": server,
            "auth_type": self.v_auth_type.get(),
            "username": self.v_username.get().strip(),
            "password": self.v_password.get(),
            "database": database,
            "table": table,
            "create_table": self.v_create_table.get(),
            "truncate": self.v_truncate.get(),
            "batch_size": int(self.v_batch_size.get() or "1000"),
        }
        self.destroy()
        
    def _on_cancel(self):
        self.result = None
        self.destroy()


class PreviewDataWindow(ModernDialog):
    """Window to display preview data in a table."""
    
    def __init__(self, parent, title="Data Preview"):
        super().__init__(parent, title, width=900, height=500)
        self._create_widgets()
        
    def _create_widgets(self):
        cf = self.content_frame
        
        # Info bar
        title_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        title_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        self.title_label = tk.Label(
            title_frame, text="Loading preview...",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            font=("Segoe UI", 11, "bold")
        )
        self.title_label.pack(side="left")
        
        self.row_count_label = tk.Label(
            title_frame, text="",
            bg=COLORS["bg_panel"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 10)
        )
        self.row_count_label.pack(side="right")
        
        # Close button at bottom
        btn_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        btn_frame.pack(fill="x", padx=10, pady=10, side="bottom")
        ModernButton(btn_frame, text="Close", primary=True, command=self._on_dialog_close).pack(side="right")
        
        # Horizontal scrollbar
        x_scroll = ttk.Scrollbar(cf, orient="horizontal")
        x_scroll.pack(fill="x", padx=10, side="bottom")
        
        # Treeview for data
        tree_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        tree_frame.pack(fill="both", expand=True, padx=10, pady=(0, 5))
        
        self.tree = ttk.Treeview(tree_frame, show="headings")
        self.tree.pack(side="left", fill="both", expand=True)
        
        # Vertical scrollbar
        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        y_scroll.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=y_scroll.set)
        
        # Configure horizontal scroll
        x_scroll.config(command=self.tree.xview)
        self.tree.configure(xscrollcommand=x_scroll.set)
        
    def set_data(self, columns, rows, table_name=""):
        """Set the data to display."""
        # Clear existing
        self.tree.delete(*self.tree.get_children())
        
        # Configure columns
        self.tree["columns"] = columns
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, minwidth=80)
            
        # Add rows
        for row in rows:
            self.tree.insert("", "end", values=row)
            
        # Update labels
        if table_name:
            self.title_label.config(text=f"Preview: {table_name}")
        else:
            self.title_label.config(text="Data Preview")
        self.row_count_label.config(text=f"{len(rows)} row(s)")
        
    def set_error(self, message):
        """Display an error message."""
        self.title_label.config(text="Preview Failed", fg=COLORS["fg_error"])
        self.row_count_label.config(text=message)


class SettingsDialog(ModernDialog):
    """Settings dialog for configuring GUI options with real-time updates."""
    
    def __init__(self, parent, settings, on_change_callback):
        self.settings = settings
        self.on_change = on_change_callback
        self.original_settings = settings.copy()
        super().__init__(parent, "Settings", width=400, height=300)
        
        self._create_widgets()
        self.bind("<Escape>", lambda e: self._on_dialog_close())
        
    def _create_widgets(self):
        cf = self.content_frame
        
        # Hint about instant changes
        hint_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        hint_frame.pack(fill="x", padx=20, pady=(10, 5))
        tk.Label(
            hint_frame, text="Changes apply instantly",
            bg=COLORS["bg_panel"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 9, "italic")
        ).pack(side="right")
        
        # UI Sections
        section1 = tk.Label(
            cf, text="Visible Sections",
            bg=COLORS["bg_panel"], fg=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold"), anchor="w"
        )
        section1.pack(fill="x", padx=20, pady=(5, 5))
        
        opt_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        opt_frame.pack(fill="x", padx=35, pady=5)
        
        self.v_show_tde = tk.BooleanVar(value=self.settings.get("show_tde", True))
        self.v_show_tde.trace_add("write", lambda *_: self._on_setting_change())
        tk.Checkbutton(
            opt_frame, text="Show TDE / Encryption section",
            variable=self.v_show_tde,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"],
            cursor="hand2"
        ).pack(anchor="w", pady=2)
        
        self.v_show_indexed = tk.BooleanVar(value=self.settings.get("show_indexed", True))
        self.v_show_indexed.trace_add("write", lambda *_: self._on_setting_change())
        tk.Checkbutton(
            opt_frame, text="Show Large Backup Mode section",
            variable=self.v_show_indexed,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"],
            cursor="hand2"
        ).pack(anchor="w", pady=2)
        
        self.v_show_filtering = tk.BooleanVar(value=self.settings.get("show_filtering", True))
        self.v_show_filtering.trace_add("write", lambda *_: self._on_setting_change())
        tk.Checkbutton(
            opt_frame, text="Show Filtering Options section",
            variable=self.v_show_filtering,
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"],
            selectcolor=COLORS["bg_input"], activebackground=COLORS["bg_panel"],
            cursor="hand2"
        ).pack(anchor="w", pady=2)
        
        # Defaults Section
        section2 = tk.Label(
            cf, text="Defaults",
            bg=COLORS["bg_panel"], fg=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold"), anchor="w"
        )
        section2.pack(fill="x", padx=20, pady=(15, 5))
        
        default_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        default_frame.pack(fill="x", padx=35, pady=5)
        
        # Default cache size
        cache_row = tk.Frame(default_frame, bg=COLORS["bg_panel"])
        cache_row.pack(fill="x", pady=2)
        tk.Label(
            cache_row, text="Default cache size (MB):",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left")
        self.v_default_cache = tk.StringVar(value=self.settings.get("default_cache", "256"))
        self.v_default_cache.trace_add("write", lambda *_: self._on_setting_change())
        ModernEntry(cache_row, textvariable=self.v_default_cache, width=8).pack(side="left", padx=(10, 0))
        
        # Default format
        fmt_row = tk.Frame(default_frame, bg=COLORS["bg_panel"])
        fmt_row.pack(fill="x", pady=5)
        tk.Label(
            fmt_row, text="Default output format:",
            bg=COLORS["bg_panel"], fg=COLORS["fg_primary"]
        ).pack(side="left")
        self.v_default_format = tk.StringVar(value=self.settings.get("default_format", "csv"))
        self.v_default_format.trace_add("write", lambda *_: self._on_setting_change())
        fmt_combo = ttk.Combobox(
            fmt_row, textvariable=self.v_default_format,
            values=["csv", "parquet", "jsonl"],
            state="readonly", width=10
        )
        fmt_combo.pack(side="left", padx=(10, 0))
        
        # Close button
        btn_frame = tk.Frame(cf, bg=COLORS["bg_panel"])
        btn_frame.pack(fill="x", padx=20, pady=15, side="bottom")
        
        ModernButton(btn_frame, text="Close", primary=True, command=self._on_dialog_close).pack(side="right")
    
    def _on_setting_change(self):
        """Apply settings in real-time."""
        self.settings["show_tde"] = self.v_show_tde.get()
        self.settings["show_indexed"] = self.v_show_indexed.get()
        self.settings["show_filtering"] = self.v_show_filtering.get()
        self.settings["default_cache"] = self.v_default_cache.get()
        self.settings["default_format"] = self.v_default_format.get()
        
        if self.on_change:
            self.on_change()


class SQLBAKReaderApp(tk.Tk):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        
        self.title("SQLBAKReader - SQL Server Backup Reader")
        self.geometry("1100x750")
        self.configure(bg=COLORS["bg_dark"])
        self.minsize(900, 600)
        
        # Window state tracking
        self._is_maximized = False
        self._normal_geometry = "1100x750+100+100"
        
        # Settings - sections hidden by default for cleaner UI
        self.app_settings = {
            "show_tde": False,
            "show_indexed": False,
            "show_filtering": False,
            "default_cache": "256",
            "default_format": "csv",
        }
        
        # Configure ttk styles
        self._setup_styles()
        
        # Build toolbar with settings
        self._build_toolbar()
        
        # Variables
        self._create_variables()
        
        # Build UI
        self._build_ui()
        
        # Update command preview
        self._update_command_preview()
        
        # Center on screen
        self.update_idletasks()
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        x = (screen_w - 1100) // 2
        y = (screen_h - 750) // 2
        self.geometry(f"1100x750+{x}+{y}")
        self._normal_geometry = f"1100x750+{x}+{y}"
        
        # Apply dark title bar on Windows
        set_dark_title_bar(self)
    
    def _build_toolbar(self):
        """Build a minimal toolbar below the dark title bar."""
        # Just a thin accent line - the title bar is now dark
        tk.Frame(self, bg=COLORS["accent"], height=2).pack(fill="x", side="top")
    
    def _open_settings(self):
        dialog = SettingsDialog(self, self.app_settings, self._apply_settings)
        self.wait_window(dialog)
    
    def _apply_settings(self):
        """Apply settings and update UI visibility."""
        # Apply default values
        if hasattr(self, 'v_cache_size'):
            cache_val = self.app_settings.get("default_cache", "256")
            if not self.v_cache_size.get():
                self.v_cache_size.set(cache_val)
        
        if hasattr(self, 'v_format'):
            fmt_val = self.app_settings.get("default_format", "csv")
            if not self.v_format.get():
                self.v_format.set(fmt_val)
        
        # Apply section visibility
        self._apply_section_visibility()
    
    def _apply_section_visibility(self):
        """Show/hide sections based on settings. Maintains correct pack order."""
        pack_opts = {"fill": "x", "pady": (0, 10)}
        
        # Right column order: options_frame, tde_frame, indexed_frame, cmd_preview_frame
        # We need to unpack toggleable frames and repack them in correct order
        
        right_col_frames = [
            ('options_frame', True),  # (name, always_visible)
            ('tde_frame', self.app_settings.get("show_tde", True)),
            ('indexed_frame', self.app_settings.get("show_indexed", True)),
            ('cmd_preview_frame', True),
        ]
        
        # Unpack all right column frames
        for frame_name, _ in right_col_frames:
            frame = getattr(self, frame_name, None)
            if frame and frame.winfo_exists():
                try:
                    frame.pack_forget()
                except tk.TclError:
                    pass
        
        # Repack in order, respecting visibility
        for frame_name, visible in right_col_frames:
            if visible:
                frame = getattr(self, frame_name, None)
                if frame and frame.winfo_exists():
                    frame.pack(**pack_opts)
        
        # Left column order: required_frame, filtering_frame
        left_col_frames = [
            ('required_frame', True),
            ('filtering_frame', self.app_settings.get("show_filtering", True)),
        ]
        
        # Unpack all left column frames
        for frame_name, _ in left_col_frames:
            frame = getattr(self, frame_name, None)
            if frame and frame.winfo_exists():
                try:
                    frame.pack_forget()
                except tk.TclError:
                    pass
        
        # Repack in order
        for frame_name, visible in left_col_frames:
            if visible:
                frame = getattr(self, frame_name, None)
                if frame and frame.winfo_exists():
                    frame.pack(**pack_opts)
        
    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use("clam")
        
        # Configure colors
        style.configure(".", 
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_primary"],
            fieldbackground=COLORS["bg_input"],
            troughcolor=COLORS["bg_dark"],
            bordercolor=COLORS["border"],
            lightcolor=COLORS["border"],
            darkcolor=COLORS["border"],
        )
        
        style.configure("TFrame", background=COLORS["bg_panel"])
        style.configure("Dark.TFrame", background=COLORS["bg_dark"])
        style.configure("Sidebar.TFrame", background=COLORS["bg_sidebar"])
        
        style.configure("TLabel", 
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_primary"]
        )
        style.configure("Sidebar.TLabel",
            background=COLORS["bg_sidebar"],
            foreground=COLORS["fg_primary"]
        )
        style.configure("Header.TLabel",
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_primary"],
            font=("Segoe UI", 12, "bold")
        )
        style.configure("Section.TLabel",
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold")
        )
        style.configure("Hint.TLabel",
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_secondary"],
            font=("Segoe UI", 9)
        )
        
        style.configure("TEntry",
            fieldbackground=COLORS["bg_input"],
            foreground=COLORS["fg_primary"],
            insertcolor=COLORS["fg_primary"],
        )
        
        style.configure("TCombobox",
            fieldbackground=COLORS["bg_input"],
            background=COLORS["bg_input"],
            foreground=COLORS["fg_primary"],
            arrowcolor=COLORS["fg_primary"],
        )
        style.map("TCombobox",
            fieldbackground=[("readonly", COLORS["bg_input"])],
            selectbackground=[("readonly", COLORS["selection"])],
        )
        
        style.configure("TCheckbutton",
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_primary"],
        )
        
        style.configure("TLabelframe",
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_accent"],
        )
        style.configure("TLabelframe.Label",
            background=COLORS["bg_panel"],
            foreground=COLORS["fg_accent"],
            font=("Segoe UI", 10, "bold")
        )
        
        style.configure("Treeview",
            background=COLORS["bg_input"],
            foreground=COLORS["fg_primary"],
            fieldbackground=COLORS["bg_input"],
        )
        style.map("Treeview",
            background=[("selected", COLORS["selection"])],
        )
        
    def _create_variables(self):
        # Backup files
        self.backup_files = []
        
        # Required
        self.v_table = tk.StringVar()
        self.v_output = tk.StringVar()
        self.v_format = tk.StringVar(value="csv")
        self.v_mode = tk.StringVar(value="auto")
        
        # Filtering
        self.v_backupset = tk.StringVar()
        self.v_columns = tk.StringVar()
        self.v_where = tk.StringVar()
        self.v_max_rows = tk.StringVar()
        self.v_delimiter = tk.StringVar(value=",")
        self.v_allocation_hint = tk.StringVar()
        
        # Connection
        self.connection_info = None
        
        # TDE
        self.v_tde_cert = tk.StringVar()
        self.v_tde_key = tk.StringVar()
        self.v_tde_password = tk.StringVar()
        
        # Options
        self.v_verbose = tk.BooleanVar(value=False)
        self.v_cleanup = tk.BooleanVar(value=False)
        
        # Indexed mode (for large backups)
        self.v_indexed = tk.BooleanVar(value=False)
        self.v_cache_size = tk.StringVar(value="256")
        self.v_index_dir = tk.StringVar()
        self.v_force_rescan = tk.BooleanVar(value=False)
        
        # Track changes
        for v in [self.v_table, self.v_output, self.v_format, self.v_mode,
                  self.v_backupset, self.v_columns, self.v_where, self.v_max_rows,
                  self.v_delimiter, self.v_allocation_hint, self.v_tde_cert,
                  self.v_tde_key, self.v_tde_password, self.v_verbose, self.v_cleanup,
                  self.v_indexed, self.v_cache_size, self.v_index_dir, self.v_force_rescan]:
            v.trace_add("write", lambda *_: self._update_command_preview())
            
    def _build_ui(self):
        # Main container
        main = ttk.Frame(self, style="Dark.TFrame")
        main.pack(fill="both", expand=True)
        
        # Sidebar
        self._build_sidebar(main)
        
        # Content area
        content = ttk.Frame(main, style="Dark.TFrame")
        content.pack(side="left", fill="both", expand=True, padx=1)
        
        # Top panel (inputs)
        top_panel = ttk.Frame(content, style="TFrame")
        top_panel.pack(fill="both", expand=True, padx=10, pady=10)
        
        self._build_input_panel(top_panel)
        
        # Bottom panel (output)
        bottom_panel = ttk.Frame(content, style="TFrame")
        bottom_panel.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        self._build_output_panel(bottom_panel)
        
    def _build_sidebar(self, parent):
        sidebar = ttk.Frame(parent, style="Sidebar.TFrame", width=220)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)
        
        # App title
        title_frame = tk.Frame(sidebar, bg=COLORS["bg_sidebar"])
        title_frame.pack(fill="x", padx=15, pady=15)
        
        tk.Label(
            title_frame, text="SQLBAKReader",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_primary"],
            font=("Segoe UI", 14, "bold")
        ).pack(anchor="w")
        
        tk.Label(
            title_frame, text="Backup Table Extractor",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 9)
        ).pack(anchor="w")
        
        # Separator
        tk.Frame(sidebar, bg=COLORS["border"], height=1).pack(fill="x", padx=15, pady=10)
        
        # Backup files section
        tk.Label(
            sidebar, text="BACKUP FILES",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 9, "bold")
        ).pack(anchor="w", padx=15, pady=(10, 5))
        
        # File list
        list_frame = tk.Frame(sidebar, bg=COLORS["bg_sidebar"])
        list_frame.pack(fill="both", expand=True, padx=15)
        
        self.file_listbox = tk.Listbox(
            list_frame,
            bg=COLORS["bg_input"], fg=COLORS["fg_primary"],
            selectbackground=COLORS["selection"],
            selectforeground=COLORS["fg_primary"],
            relief="flat", highlightthickness=1,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["fg_accent"],
            font=("Consolas", 9)
        )
        self.file_listbox.pack(fill="both", expand=True)
        
        # File buttons
        btn_frame = tk.Frame(sidebar, bg=COLORS["bg_sidebar"])
        btn_frame.pack(fill="x", padx=15, pady=10)
        
        ModernButton(
            btn_frame, text="Add Files", primary=True,
            command=self._add_files
        ).pack(side="left")
        
        ModernButton(
            btn_frame, text="Clear",
            command=self._clear_files
        ).pack(side="left", padx=(8, 0))
        
        # Separator
        tk.Frame(sidebar, bg=COLORS["border"], height=1).pack(fill="x", padx=15, pady=10)
        
        # Connection section
        tk.Label(
            sidebar, text="TARGET SERVER",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 9, "bold")
        ).pack(anchor="w", padx=15, pady=(10, 5))
        
        self.connection_label = tk.Label(
            sidebar, text="Not connected",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 9)
        )
        self.connection_label.pack(anchor="w", padx=15)
        
        conn_btn_frame = tk.Frame(sidebar, bg=COLORS["bg_sidebar"])
        conn_btn_frame.pack(fill="x", padx=15, pady=10)
        
        ModernButton(
            conn_btn_frame, text="Configure...", primary=True,
            command=self._configure_connection
        ).pack(side="left")
        
        ModernButton(
            conn_btn_frame, text="Clear",
            command=self._clear_connection
        ).pack(side="left", padx=(8, 0))
        
        # Hint for restore mode
        tk.Label(
            sidebar, text="Required for Restore mode",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 8, "italic")
        ).pack(anchor="w", padx=15)
        
        # Spacer to push settings to bottom
        tk.Frame(sidebar, bg=COLORS["bg_sidebar"]).pack(fill="both", expand=True)
        
        # Settings button at bottom
        tk.Frame(sidebar, bg=COLORS["border"], height=1).pack(fill="x", padx=15, pady=5)
        
        settings_frame = tk.Frame(sidebar, bg=COLORS["bg_sidebar"])
        settings_frame.pack(fill="x", padx=15, pady=10)
        
        settings_btn = tk.Label(
            settings_frame, text="\u2699  Settings",
            bg=COLORS["bg_sidebar"], fg=COLORS["fg_secondary"],
            font=("Segoe UI", 10), cursor="hand2", anchor="w"
        )
        settings_btn.pack(fill="x")
        settings_btn.bind("<Enter>", lambda e: settings_btn.config(fg=COLORS["fg_primary"]))
        settings_btn.bind("<Leave>", lambda e: settings_btn.config(fg=COLORS["fg_secondary"]))
        settings_btn.bind("<Button-1>", lambda e: self._open_settings())
        
    def _build_input_panel(self, parent):
        # Store parent for rebuilding
        self.input_panel_parent = parent
        
        # Create two columns
        left_col = ttk.Frame(parent)
        left_col.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        right_col = ttk.Frame(parent)
        right_col.pack(side="left", fill="both", expand=True)
        
        # Store column references
        self.left_col = left_col
        self.right_col = right_col
        
        # Left column: Required options
        self.required_frame = self._build_required_section(left_col)
        self.filtering_frame = self._build_filtering_section(left_col)
        
        # Right column: Options, TDE, Indexed mode, command preview
        self.options_frame = self._build_options_section(right_col)
        self.tde_frame = self._build_tde_section(right_col)
        self.indexed_frame = self._build_indexed_section(right_col)
        self.cmd_preview_frame = self._build_command_preview(right_col)
        
        # Apply initial settings visibility
        self._apply_section_visibility()
        
    def _build_required_section(self, parent):
        frame = ttk.LabelFrame(parent, text="Extraction Settings", padding=10)
        frame.pack(fill="x", pady=(0, 10))
        
        # Table name
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=4)
        ttk.Label(row, text="Table:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Entry(row, textvariable=self.v_table, width=35).pack(side="left", fill="x", expand=True)
        
        ttk.Label(frame, text="Format: schema.table (e.g., dbo.Orders)", 
                  style="Hint.TLabel").pack(anchor="w", padx=(100, 0))
        
        # Output file
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=4)
        ttk.Label(row, text="Output file:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Entry(row, textvariable=self.v_output, width=35).pack(side="left", fill="x", expand=True)
        ModernButton(row, text="Browse", command=self._browse_output).pack(side="left", padx=(4, 0))
        
        # Format and Mode
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=4)
        
        ttk.Label(row, text="Format:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Combobox(
            row, textvariable=self.v_format,
            values=["csv", "parquet", "jsonl"],
            state="readonly", width=12
        ).pack(side="left")
        
        ttk.Label(row, text="Mode:", width=10, anchor="e").pack(side="left", padx=(20, 8))
        mode_combo = ttk.Combobox(
            row, textvariable=self.v_mode,
            values=["auto", "direct", "restore"],
            state="readonly", width=12
        )
        mode_combo.pack(side="left")
        mode_combo.bind("<<ComboboxSelected>>", self._on_mode_change)
        
        return frame
        
    def _build_filtering_section(self, parent):
        frame = ttk.LabelFrame(parent, text="Filtering Options", padding=10)
        frame.pack(fill="x", pady=(0, 10))
        
        fields = [
            ("Backup set #:", self.v_backupset, "Position in multi-backup file"),
            ("Columns:", self.v_columns, "Comma-separated list"),
            ("WHERE clause:", self.v_where, "SQL condition (restore mode)"),
            ("Max rows:", self.v_max_rows, "Limit output rows"),
            ("Delimiter:", self.v_delimiter, "CSV delimiter"),
        ]
        
        for label, var, hint in fields:
            row = ttk.Frame(frame)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=label, width=14, anchor="e").pack(side="left", padx=(0, 8))
            ttk.Entry(row, textvariable=var, width=35).pack(side="left", fill="x", expand=True)
            
        # Allocation hint
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text="Alloc hint CSV:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Entry(row, textvariable=self.v_allocation_hint, width=35).pack(side="left", fill="x", expand=True)
        ModernButton(row, text="Browse", command=self._browse_allocation_hint).pack(side="left", padx=(4, 0))
        
        return frame
        
    def _build_tde_section(self, parent):
        frame = ttk.LabelFrame(parent, text="TDE / Encryption", padding=10)
        frame.pack(fill="x", pady=(0, 10))
        
        ttk.Label(
            frame, 
            text="For TDE-encrypted backups, provide the certificate exported from the source server.",
            style="Hint.TLabel", wraplength=350
        ).pack(anchor="w", pady=(0, 8))
        
        # Certificate file
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text="Certificate:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Entry(row, textvariable=self.v_tde_cert, width=30).pack(side="left", fill="x", expand=True)
        ModernButton(row, text="Browse", command=lambda: self._browse_file(
            self.v_tde_cert, [("Certificate", "*.cer;*.pfx"), ("All", "*.*")]
        )).pack(side="left", padx=(4, 0))
        
        # Private key
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text="Private key:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Entry(row, textvariable=self.v_tde_key, width=30).pack(side="left", fill="x", expand=True)
        ModernButton(row, text="Browse", command=lambda: self._browse_file(
            self.v_tde_key, [("Private Key", "*.pvk"), ("All", "*.*")]
        )).pack(side="left", padx=(4, 0))
        
        # Password
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=2)
        ttk.Label(row, text="Key password:", width=14, anchor="e").pack(side="left", padx=(0, 8))
        ttk.Entry(row, textvariable=self.v_tde_password, width=30, show="*").pack(side="left", fill="x", expand=True)
        
        return frame
        
    def _build_options_section(self, parent):
        frame = ttk.LabelFrame(parent, text="Options", padding=10)
        frame.pack(fill="x", pady=(0, 10))
        
        row1 = ttk.Frame(frame)
        row1.pack(fill="x")
        
        ttk.Checkbutton(row1, text="Verbose output", variable=self.v_verbose).pack(side="left")
        ttk.Checkbutton(row1, text="Cleanup keys after extraction", variable=self.v_cleanup).pack(side="left", padx=(20, 0))
        
        return frame
    
    def _build_indexed_section(self, parent):
        """Build the indexed mode section for large backups."""
        frame = ttk.LabelFrame(parent, text="Large Backup Mode (Indexed)", padding=10)
        frame.pack(fill="x", pady=(0, 10))
        
        row2 = ttk.Frame(frame)
        row2.pack(fill="x", pady=(0, 5))
        
        ttk.Checkbutton(row2, text="Use indexed mode (recommended for >1GB backups)", 
                        variable=self.v_indexed).pack(side="left")
        ttk.Checkbutton(row2, text="Force rescan", variable=self.v_force_rescan).pack(side="left", padx=(20, 0))
        
        row3 = ttk.Frame(frame)
        row3.pack(fill="x", pady=(0, 5))
        
        ttk.Label(row3, text="Cache size (MB):").pack(side="left")
        cache_entry = ttk.Entry(row3, textvariable=self.v_cache_size, width=8)
        cache_entry.pack(side="left", padx=(5, 20))
        
        ttk.Label(row3, text="Index directory:").pack(side="left")
        ttk.Entry(row3, textvariable=self.v_index_dir, width=30).pack(side="left", padx=(5, 5), fill="x", expand=True)
        ModernButton(row3, text="...", command=self._browse_index_dir).pack(side="left")
        
        hint = ttk.Label(frame, text="Indexed mode scans backup once and builds an index for fast random page access. " +
                        "Uses LRU cache instead of loading all pages into memory.", style="Hint.TLabel")
        hint.pack(fill="x")
        
        return frame
    
    def _browse_index_dir(self):
        path = filedialog.askdirectory(title="Select Index Directory")
        if path:
            self.v_index_dir.set(path)
        
    def _build_command_preview(self, parent):
        frame = ttk.LabelFrame(parent, text="Command Preview", padding=10)
        frame.pack(fill="x", pady=(0, 10))
        
        self.cmd_preview = tk.Text(
            frame, height=3, wrap="word",
            bg=COLORS["bg_input"], fg=COLORS["fg_primary"],
            font=("Consolas", 9), relief="flat",
            highlightthickness=1, highlightbackground=COLORS["border"]
        )
        self.cmd_preview.pack(fill="x")
        self.cmd_preview.config(state="disabled")
        
        return frame
        
    def _build_output_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="Output", padding=10)
        frame.pack(fill="both", expand=True)
        
        # Output text
        text_frame = ttk.Frame(frame)
        text_frame.pack(fill="both", expand=True)
        
        self.output_text = tk.Text(
            text_frame, wrap="word",
            bg=COLORS["bg_input"], fg=COLORS["fg_primary"],
            font=("Consolas", 10), relief="flat",
            highlightthickness=1, highlightbackground=COLORS["border"]
        )
        self.output_text.pack(side="left", fill="both", expand=True)
        
        scrollbar = ttk.Scrollbar(text_frame, command=self.output_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.output_text.config(yscrollcommand=scrollbar.set)
        
        # Tags for colored output
        self.output_text.tag_configure("info", foreground=COLORS["fg_primary"])
        self.output_text.tag_configure("success", foreground=COLORS["fg_success"])
        self.output_text.tag_configure("error", foreground=COLORS["fg_error"])
        self.output_text.tag_configure("warning", foreground=COLORS["fg_warning"])
        
        # Buttons - Row 1 (primary actions)
        btn_frame1 = ttk.Frame(frame)
        btn_frame1.pack(fill="x", pady=(10, 0))
        
        self.run_btn = ModernButton(
            btn_frame1, text="Extract to File", primary=True,
            command=self._run_extraction
        )
        self.run_btn.pack(side="left")
        
        ModernButton(
            btn_frame1, text="Preview Data",
            command=self._preview_data
        ).pack(side="left", padx=(8, 0))
        
        ModernButton(
            btn_frame1, text="Export to Database",
            command=self._export_to_database
        ).pack(side="left", padx=(8, 0))
        
        # Buttons - Row 2 (secondary actions)
        btn_frame2 = ttk.Frame(frame)
        btn_frame2.pack(fill="x", pady=(5, 0))
        
        ModernButton(
            btn_frame2, text="List Tables",
            command=self._list_tables
        ).pack(side="left")
        
        ModernButton(
            btn_frame2, text="Clear Output",
            command=self._clear_output
        ).pack(side="left", padx=(8, 0))
        
        # Status
        self.status_label = ttk.Label(btn_frame2, text="Ready", style="Hint.TLabel")
        self.status_label.pack(side="right")
        
    # ── Actions ─────────────────────────────────────────────────────
    
    def _add_files(self):
        files = filedialog.askopenfilenames(
            filetypes=[("SQL Backup", "*.bak"), ("All files", "*.*")]
        )
        for f in files:
            if f not in self.backup_files:
                self.backup_files.append(f)
                self.file_listbox.insert("end", Path(f).name)
        self._update_command_preview()
        
    def _clear_files(self):
        self.backup_files.clear()
        self.file_listbox.delete(0, "end")
        self._update_command_preview()
        
    def _configure_connection(self):
        dialog = ConnectionDialog(self, self.connection_info)
        self.wait_window(dialog)
        
        if dialog.result:
            self.connection_info = dialog.result
            server = self.connection_info["server"]
            auth = "Windows" if self.connection_info["auth_type"] == "Windows Authentication" else "SQL"
            self.connection_label.config(
                text=f"{server}\n({auth} Auth)",
                fg=COLORS["fg_success"]
            )
        self._update_command_preview()
        
    def _clear_connection(self):
        self.connection_info = None
        self.connection_label.config(text="Not connected", fg=COLORS["fg_secondary"])
        self._update_command_preview()
        
    def _on_mode_change(self, event=None):
        mode = self.v_mode.get()
        if mode == "restore" and not self.connection_info:
            messagebox.showinfo(
                "Restore Mode",
                "Restore mode requires a target SQL Server.\n\n"
                "Click 'Configure...' in the sidebar to set up the connection.",
                parent=self
            )
            
    def _browse_output(self):
        fmt = self.v_format.get()
        ext_map = {"csv": ".csv", "parquet": ".parquet", "jsonl": ".jsonl"}
        ext = ext_map.get(fmt, ".csv")
        
        path = filedialog.asksaveasfilename(
            filetypes=[
                ("CSV", "*.csv"), ("Parquet", "*.parquet"),
                ("JSON Lines", "*.jsonl"), ("All", "*.*")
            ],
            defaultextension=ext
        )
        if path:
            self.v_output.set(path)
            
    def _browse_allocation_hint(self):
        path = filedialog.askopenfilename(
            filetypes=[("CSV", "*.csv"), ("All", "*.*")]
        )
        if path:
            self.v_allocation_hint.set(path)
            
    def _browse_file(self, var, filetypes):
        path = filedialog.askopenfilename(filetypes=filetypes)
        if path:
            var.set(path)
            
    def _update_command_preview(self):
        cmd = self._build_command()
        
        self.cmd_preview.config(state="normal")
        self.cmd_preview.delete("1.0", "end")
        self.cmd_preview.insert("1.0", " ".join(cmd))
        self.cmd_preview.config(state="disabled")
        
    def _build_command(self):
        cmd = [find_bakread()]
        
        # Backup files
        for f in self.backup_files:
            cmd += ["--bak", f]
            
        # Required
        table = self.v_table.get().strip()
        if table:
            cmd += ["--table", table]
            
        output = self.v_output.get().strip()
        if output:
            cmd += ["--out", output]
            
        cmd += ["--format", self.v_format.get()]
        cmd += ["--mode", self.v_mode.get()]
        
        # Filtering
        if self.v_backupset.get().strip():
            cmd += ["--backupset", self.v_backupset.get().strip()]
        if self.v_columns.get().strip():
            cmd += ["--columns", self.v_columns.get().strip()]
        if self.v_where.get().strip():
            cmd += ["--where", self.v_where.get().strip()]
        if self.v_max_rows.get().strip():
            cmd += ["--max-rows", self.v_max_rows.get().strip()]
        if self.v_delimiter.get().strip() and self.v_delimiter.get().strip() != ",":
            cmd += ["--delimiter", self.v_delimiter.get().strip()]
        if self.v_allocation_hint.get().strip():
            cmd += ["--allocation-hint", self.v_allocation_hint.get().strip()]
            
        # Connection
        if self.connection_info:
            cmd += ["--target-server", self.connection_info["server"]]
            # Add SQL auth credentials (username only shown in preview, password via env)
            if self.connection_info["auth_type"] == "SQL Server Authentication":
                if self.connection_info.get("username"):
                    cmd += ["--sql-user", self.connection_info["username"]]
                # Password passed via environment variable for security
            
        # TDE
        if self.v_tde_cert.get().strip():
            cmd += ["--tde-cert-pfx", self.v_tde_cert.get().strip()]
        if self.v_tde_key.get().strip():
            cmd += ["--tde-cert-key", self.v_tde_key.get().strip()]
        if self.v_tde_password.get().strip():
            cmd += ["--tde-cert-password", self.v_tde_password.get().strip()]
            
        # Options
        if self.v_verbose.get():
            cmd += ["--verbose"]
        if self.v_cleanup.get():
            cmd += ["--cleanup-keys"]
        
        # Indexed mode
        if self.v_indexed.get():
            cmd += ["--indexed"]
            cache = self.v_cache_size.get().strip()
            if cache and cache != "256":
                cmd += ["--cache-size", cache]
            idx_dir = self.v_index_dir.get().strip()
            if idx_dir:
                cmd += ["--index-dir", idx_dir]
            if self.v_force_rescan.get():
                cmd += ["--force-rescan"]
            
        return cmd
        
    def _run_extraction(self):
        if not self.backup_files:
            messagebox.showerror("Error", "Please add at least one backup file.", parent=self)
            return
            
        if not self.v_table.get().strip():
            messagebox.showerror("Error", "Please specify a table name.", parent=self)
            return
            
        if not self.v_output.get().strip():
            messagebox.showerror("Error", "Please specify an output file.", parent=self)
            return
            
        mode = self.v_mode.get()
        if mode == "restore" and not self.connection_info:
            messagebox.showerror(
                "Error", 
                "Restore mode requires a target SQL Server.\n"
                "Click 'Configure...' to set up the connection.",
                parent=self
            )
            return
            
        self._execute_command(self._build_command())
        
    def _list_tables(self):
        if not self.backup_files:
            messagebox.showerror("Error", "Please add at least one backup file.", parent=self)
            return
            
        cmd = [find_bakread()]
        for f in self.backup_files:
            cmd += ["--bak", f]
        cmd += ["--list-tables"]
        
        # Add target server if configured (for restore mode list)
        if self.connection_info:
            cmd += ["--target-server", self.connection_info["server"]]
            if self.connection_info["auth_type"] == "SQL Server Authentication":
                if self.connection_info.get("username"):
                    cmd += ["--sql-user", self.connection_info["username"]]
        
        if self.v_verbose.get():
            cmd += ["--verbose"]
            
        self._execute_command(cmd)
    
    def _preview_data(self):
        """Preview first N rows of the selected table."""
        if not self.backup_files:
            messagebox.showerror("Error", "Please add at least one backup file.", parent=self)
            return
            
        table = self.v_table.get().strip()
        if not table:
            messagebox.showerror("Error", "Please specify a table name.", parent=self)
            return
        
        # Create preview window
        preview_win = PreviewDataWindow(self, f"Preview: {table}")
        
        # Build command for CSV output
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        temp_path = temp_file.name
        temp_file.close()
        
        cmd = [find_bakread()]
        for f in self.backup_files:
            cmd += ["--bak", f]
        cmd += ["--table", table]
        cmd += ["--out", temp_path]
        cmd += ["--format", "csv"]
        cmd += ["--max-rows", "100"]  # Preview limit
        cmd += ["--mode", self.v_mode.get()]
        
        # Add connection info
        if self.connection_info:
            cmd += ["--target-server", self.connection_info["server"]]
            if self.connection_info["auth_type"] == "SQL Server Authentication":
                if self.connection_info.get("username"):
                    cmd += ["--sql-user", self.connection_info["username"]]
        
        # Add TDE if configured
        if self.v_tde_cert.get().strip():
            cmd += ["--tde-cert-pfx", self.v_tde_cert.get().strip()]
        if self.v_tde_key.get().strip():
            cmd += ["--tde-cert-key", self.v_tde_key.get().strip()]
        if self.v_tde_password.get().strip():
            cmd += ["--tde-cert-password", self.v_tde_password.get().strip()]
        
        # Indexed mode
        if self.v_indexed.get():
            cmd += ["--indexed"]
            cache = self.v_cache_size.get().strip()
            if cache and cache != "256":
                cmd += ["--cache-size", cache]
        
        def run_preview():
            try:
                env = os.environ.copy()
                if self.connection_info and self.connection_info["auth_type"] == "SQL Server Authentication":
                    env["BAKREAD_SQL_USER"] = self.connection_info.get("username", "")
                    env["BAKREAD_SQL_PASSWORD"] = self.connection_info.get("password", "")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env
                )
                stdout, stderr = process.communicate(timeout=120)
                
                if process.returncode == 0:
                    # Parse CSV file
                    with open(temp_path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        rows = list(reader)
                    
                    if rows:
                        columns = rows[0]
                        data = rows[1:]
                        self.after(0, lambda: preview_win.set_data(columns, data, table))
                    else:
                        self.after(0, lambda: preview_win.set_error("No data returned"))
                else:
                    error_msg = stderr.strip() if stderr else f"Exit code {process.returncode}"
                    self.after(0, lambda: preview_win.set_error(error_msg[:200]))
                    
            except subprocess.TimeoutExpired:
                process.kill()
                self.after(0, lambda: preview_win.set_error("Preview timed out"))
            except Exception as e:
                self.after(0, lambda: preview_win.set_error(str(e)[:200]))
            finally:
                # Cleanup temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
        
        threading.Thread(target=run_preview, daemon=True).start()
    
    def _export_to_database(self):
        """Export extracted data to a SQL Server database."""
        if not self.backup_files:
            messagebox.showerror("Error", "Please add at least one backup file.", parent=self)
            return
            
        table = self.v_table.get().strip()
        if not table:
            messagebox.showerror("Error", "Please specify a source table name.", parent=self)
            return
        
        # Show export dialog
        dialog = ExportDatabaseDialog(self, source_table=table)
        self.wait_window(dialog)
        
        if not dialog.result:
            return
        
        export_config = dialog.result
        
        # First extract to temp CSV, then bulk insert
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        temp_path = temp_file.name
        temp_file.close()
        
        self._append_output(f"Exporting {table} to {export_config['server']}/{export_config['database']}.{export_config['table']}...\n")
        self.status_label.config(text="Exporting...")
        self.run_btn.config(state="disabled")
        
        def run_export():
            try:
                # Step 1: Extract to temp CSV
                self._append_output("Step 1: Extracting data from backup...\n")
                
                cmd = [find_bakread()]
                for f in self.backup_files:
                    cmd += ["--bak", f]
                cmd += ["--table", table]
                cmd += ["--out", temp_path]
                cmd += ["--format", "csv"]
                cmd += ["--mode", self.v_mode.get()]
                
                # Source connection
                if self.connection_info:
                    cmd += ["--target-server", self.connection_info["server"]]
                    if self.connection_info["auth_type"] == "SQL Server Authentication":
                        if self.connection_info.get("username"):
                            cmd += ["--sql-user", self.connection_info["username"]]
                
                # TDE
                if self.v_tde_cert.get().strip():
                    cmd += ["--tde-cert-pfx", self.v_tde_cert.get().strip()]
                if self.v_tde_key.get().strip():
                    cmd += ["--tde-cert-key", self.v_tde_key.get().strip()]
                if self.v_tde_password.get().strip():
                    cmd += ["--tde-cert-password", self.v_tde_password.get().strip()]
                
                # Indexed mode
                if self.v_indexed.get():
                    cmd += ["--indexed"]
                
                env = os.environ.copy()
                if self.connection_info and self.connection_info["auth_type"] == "SQL Server Authentication":
                    env["BAKREAD_SQL_USER"] = self.connection_info.get("username", "")
                    env["BAKREAD_SQL_PASSWORD"] = self.connection_info.get("password", "")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env
                )
                
                for line in process.stdout:
                    self._append_output(line)
                
                process.wait()
                
                if process.returncode != 0:
                    self._append_output("\nExtraction failed. Export aborted.\n", "error")
                    return
                
                # Step 2: Bulk insert to destination database
                self._append_output("\nStep 2: Importing data to destination database...\n")
                
                # Build connection string for pyodbc
                try:
                    import pyodbc
                except ImportError:
                    self._append_output("Error: pyodbc is required for database export. Install with: pip install pyodbc\n", "error")
                    return
                
                # Build connection string
                if export_config["auth_type"] == "Windows Authentication":
                    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={export_config['server']};DATABASE={export_config['database']};Trusted_Connection=yes;TrustServerCertificate=yes"
                else:
                    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={export_config['server']};DATABASE={export_config['database']};UID={export_config['username']};PWD={export_config['password']};TrustServerCertificate=yes"
                
                try:
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                    
                    # Read CSV
                    with open(temp_path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        headers = next(reader)
                        rows = list(reader)
                    
                    dest_table = export_config['table']
                    
                    # Create table if needed
                    if export_config['create_table']:
                        # Build CREATE TABLE (all NVARCHAR for simplicity)
                        cols = ", ".join([f"[{h}] NVARCHAR(MAX)" for h in headers])
                        create_sql = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{dest_table.split('.')[-1]}') CREATE TABLE {dest_table} ({cols})"
                        try:
                            cursor.execute(create_sql)
                            conn.commit()
                            self._append_output(f"Created table {dest_table}\n")
                        except Exception as e:
                            self._append_output(f"Warning: Could not create table: {e}\n", "warning")
                    
                    # Truncate if requested
                    if export_config['truncate']:
                        try:
                            cursor.execute(f"TRUNCATE TABLE {dest_table}")
                            conn.commit()
                            self._append_output(f"Truncated table {dest_table}\n")
                        except Exception as e:
                            self._append_output(f"Warning: Could not truncate: {e}\n", "warning")
                    
                    # Bulk insert
                    placeholders = ", ".join(["?" for _ in headers])
                    insert_sql = f"INSERT INTO {dest_table} ({', '.join([f'[{h}]' for h in headers])}) VALUES ({placeholders})"
                    
                    batch_size = export_config['batch_size']
                    total_rows = len(rows)
                    inserted = 0
                    
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i+batch_size]
                        cursor.executemany(insert_sql, batch)
                        conn.commit()
                        inserted += len(batch)
                        self._append_output(f"Inserted {inserted}/{total_rows} rows...\n")
                    
                    cursor.close()
                    conn.close()
                    
                    self._append_output(f"\nExport completed: {total_rows} rows inserted into {dest_table}\n", "success")
                    
                except pyodbc.Error as e:
                    self._append_output(f"\nDatabase error: {e}\n", "error")
                except Exception as e:
                    self._append_output(f"\nError during import: {e}\n", "error")
                    
            except Exception as e:
                self._append_output(f"\nExport error: {e}\n", "error")
            finally:
                # Cleanup temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
                self.after(0, lambda: self.run_btn.config(state="normal"))
                self.after(0, lambda: self.status_label.config(text="Ready"))
        
        threading.Thread(target=run_export, daemon=True).start()
        
    def _execute_command(self, cmd):
        self._clear_output()
        self.run_btn.config(state="disabled")
        self.status_label.config(text="Running...")
        
        def run():
            try:
                # Set up environment for SQL auth if needed
                env = os.environ.copy()
                if self.connection_info and self.connection_info["auth_type"] == "SQL Server Authentication":
                    env["BAKREAD_SQL_USER"] = self.connection_info["username"]
                    env["BAKREAD_SQL_PASSWORD"] = self.connection_info["password"]
                    
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    env=env
                )
                
                for line in process.stdout:
                    self._append_output(line)
                    
                process.wait()
                
                if process.returncode == 0:
                    self._append_output("\n[Completed successfully]\n", "success")
                else:
                    self._append_output(f"\n[Failed with exit code {process.returncode}]\n", "error")
                    
            except Exception as e:
                self._append_output(f"\nError: {e}\n", "error")
            finally:
                self.after(0, lambda: self.run_btn.config(state="normal"))
                self.after(0, lambda: self.status_label.config(text="Ready"))
                
        threading.Thread(target=run, daemon=True).start()
        
    def _append_output(self, text, tag="info"):
        def do_append():
            self.output_text.config(state="normal")
            
            # Detect log level
            if "[ERROR]" in text or "error" in text.lower():
                tag_to_use = "error"
            elif "[WARN]" in text:
                tag_to_use = "warning"
            elif "SUCCESS" in text or "success" in text.lower():
                tag_to_use = "success"
            else:
                tag_to_use = tag
                
            self.output_text.insert("end", text, tag_to_use)
            self.output_text.see("end")
            self.output_text.config(state="disabled")
            
        self.after(0, do_append)
        
    def _clear_output(self):
        self.output_text.config(state="normal")
        self.output_text.delete("1.0", "end")
        self.output_text.config(state="disabled")


def main():
    app = SQLBAKReaderApp()
    app.mainloop()


if __name__ == "__main__":
    main()
