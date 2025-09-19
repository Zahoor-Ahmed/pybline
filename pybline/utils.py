"""
utils.py

This module provides utility functions for logging, output formatting, and time-related
operations useful in querying and exporting data in Big Data environments.
"""
import time
import datetime
import pandas as pd # type: ignore
import re
# import glob
from importlib.resources import files
from pathlib import Path

import os
os.environ['PYGAME_HIDE_SUPPORT_PROMPT'] = "hide"
import pygame # type: ignore
def alert(index=0):
    """
    Play a system beep sound as a simple alert.
    Useful for notifying users after long-running operations.
    Falls back silently if audio is unavailable.
    """
    try:
        pygame.mixer.init()
        time.sleep(0.2)  # Give it a moment to initialize
        base = Path(__file__).parent / "sounds"
        sound_files = sorted(base.glob("*.wav"))
        if not sound_files:
            print("No sound files found.")
            return
        sound = pygame.mixer.Sound(str(sound_files[index]))
        sound.play()
    except Exception as e:
        pass
        # print("Failed to play sound:", e)


def to_sql_inlist(series):
    """Convert a Pandas Series to a SQL-compatible IN clause string."""
    return ','.join("'" + series.dropna().astype(str) + "'")

def todayx(r=7, ref=None):
    """
    Print a list of epoch days leading up to the reference date.

    Args:
        r (int): Number of days to look back.
        ref (int or None): Epoch day to use as reference. If None, today is used.
    """
    from datetime import datetime, timedelta
    if ref is None:
        ref = datetime.now().date()
    else:
        # Assuming ref is given as an epoch day, convert it to a date
        ref = (datetime(1970, 1, 1) + timedelta(days=ref)).date()

    start_date = ref - timedelta(days=r)
    for i in range(r + 1):
        sys_date = start_date + timedelta(days=i)
        unix_date = (sys_date - datetime(1970, 1, 1).date()).days

        if sys_date == ref:
            print("-------------------------")
            print(f"{sys_date}   {unix_date}")
            print("-------------------------")
        else:
            print(f"{sys_date}   {unix_date}   {i - r}")

from datetime import datetime, timedelta

def this_monthx(r=7, ref=None):
    """
    Print a list of epoch months leading up to the reference month.

    Args:
        r (int): Number of months to look back.
        ref (int or None): Epoch month to use as reference. If None, current month is used.
    """
    if ref is None:
        ref = datetime.now()
    else:
        # Assuming ref is given as a month code, convert it to a date
        years = ref // 12 + 1970
        months = ref % 12 + 1
        ref = datetime(years, months, 1)

    # Iterate through the months, starting from `r` months before the reference month
    for i in range(r, -1, -1):
        # Calculate the month by subtracting months from the reference date
        month_adjusted = datetime(ref.year, ref.month, 1) - timedelta(days=30 * i)
        month_code = (month_adjusted.year - 1970) * 12 + month_adjusted.month - 1
        month_name = f"{month_adjusted.strftime('%b')}-{month_adjusted.year}"

        if i == 0:
            print("-------------------------")
            print(f"{month_name}   {month_code}")
            print("-------------------------")
        else:
            print(f"{month_name}   {month_code}   {-i}")


from datetime import datetime, timedelta
from calendar import monthrange

def daypartitions(epoch_month):
    """
    Given an epoch month (number of months since Jan 1970),
    returns a list of epoch day values for each day in that month.
    """
    # Convert epoch_month to year and month
    years_since_1970, month_offset = divmod(epoch_month, 12)
    year = 1970 + years_since_1970
    month = month_offset + 1  # calendar module uses 1-based months

    # Determine the number of days in the month
    _, num_days = monthrange(year, month)
    
    # Generate list of epoch days for the month
    start_date = datetime(year, month, 1)
    epoch_base = datetime(1970, 1, 1)
    return [(start_date + timedelta(days=i) - epoch_base).days for i in range(num_days)]


def daypartitions_to_sec(day_list):
    return [int(time.mktime(time.strptime(day, "%Y%m%d"))) for day in day_list]


def _trim_zwsp_and_whitespace(text):
    """
    Trim both regular whitespace and all zero-width characters.
    Optimized for vectorized operations with pandas.
    """
    if pd.isna(text) or text is None:
        return text
    
    text_str = str(text)
    
    # Remove all zero-width characters (including ZWSP, ZWNJ, ZWJ, etc.)
    zero_width_chars = [
        '\u200b',  # Zero Width Space
        '\u200c',  # Zero Width Non-Joiner
        '\u200d',  # Zero Width Joiner
        '\u2060',  # Word Joiner
        '\ufeff',  # Zero Width No-Break Space (BOM)
    ]
    
    for char in zero_width_chars:
        text_str = text_str.replace(char, '')
    
    # Trim regular whitespace
    return text_str.strip()


def text_to_df(output):
    """
    Convert raw text output from Beeline SQL execution into a Pandas DataFrame.
    Optimized for large datasets with efficient space and ZWSP trimming.

    Args:
        output (str): Raw output string from Beeline query execution.

    Returns:
        pd.DataFrame: DataFrame representation of the query result with all spaces and ZWSP trimmed.
    """
    lines = output.strip().splitlines()
    divider_regex = re.compile(r'^[\+\-]+$', re.UNICODE)
    processed_lines = []
    header = None
    counter = 1

    for line in lines:
        if divider_regex.match(line.strip()) and counter == 1:
            counter += 1
            continue
        if counter == 2:
            counter += 1
            # Efficiently trim header columns including ZWSP
            header = [_trim_zwsp_and_whitespace(col) for col in line.strip('| ').split(' | ')]
            continue
        if divider_regex.match(line.strip()) and counter == 3:
            counter += 1
            continue
        if divider_regex.match(line.strip()) and counter == 4:
            counter = 2
            continue
        else:
            # Efficiently trim data row columns including ZWSP
            row = [_trim_zwsp_and_whitespace(cell) for cell in line.strip('|').split(' | ')]
            if len(row) < len(header):
                row += [None] * (len(header) - len(row))
            if len(row) == len(header):
                processed_lines.append(row)
    
    # Create DataFrame and apply comprehensive trimming to all string columns
    df = pd.DataFrame(processed_lines, columns=header)
    
    # Fast trimming for all string columns - much faster than applying to each cell
    if not df.empty:
        # Define all zero-width characters to remove
        zero_width_chars = ['\u200b', '\u200c', '\u200d', '\u2060', '\ufeff']
        
        # Trim column names including all zero-width characters
        for char in zero_width_chars:
            df.columns = df.columns.str.replace(char, '', regex=False)
        df.columns = df.columns.str.strip()
        
        # Trim all string values in the DataFrame efficiently including all zero-width characters
        # This is much faster than iterating through each cell
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                # Remove all zero-width characters and trim whitespace
                for char in zero_width_chars:
                    df[col] = df[col].astype(str).str.replace(char, '', regex=False)
                df[col] = df[col].str.strip()
                # Convert 'None' strings back to None for consistency
                df[col] = df[col].replace('None', None)
    
    return df


def clean_sql(sql_query):
    sql_query = re.sub(r' +', ' ', sql_query)
    sql_query = re.sub(r'\n\s*\n', '\n', sql_query)
    sql_query = re.sub(r'\n+', '\n', sql_query)
    sql_query = re.sub(r'^ +', '', sql_query, flags=re.MULTILINE)
    sql_query = sql_query.replace('\t', '')
    sql_query = sql_query.replace('\r', '')
    sql_query = sql_query.replace('\f', '')
    sql_query = sql_query.replace('\v', '')
    sql_query = re.sub(r'[^\S\n]*,[^\S\n]*', ',', sql_query)
    sql_query = re.sub(r'\s*;\s*$', '', sql_query)
    return sql_query


from tkinter.filedialog import asksaveasfilename
from tkinter import Tk

def export(output):
    df = text_to_df(output)

    # Hide the root tkinter window
    root = Tk()
    root.withdraw()
    root.lift()
    root.attributes("-topmost", True)
    root.after(0, root.focus_force)

    # Prompt the user to select a file save location
    file_path = asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        title="Save DataFrame as CSV"
    )

    # If the user selects a file, save the DataFrame to the specified path
    if file_path:
        df.to_csv(file_path, index=False)
        print(f"DataFrame saved to {file_path}")
    else:
        print("Save operation cancelled.")


def set_env(interactive=True):
    """
    Set up the pybline configuration. Creates ~/.pybline_config.json either interactively
    or with placeholder values. Prompts before overwriting if config already exists.
    SSH_CONFIG and WINSCP_CONFIG share server, username, and password fields.

    Args:
        interactive (bool): If True, prompts user for config. If False, uses placeholders.
    """
    import os, json, webbrowser, platform, subprocess

    path = os.path.normpath(os.path.expanduser("~/.pybline_config.json"))

    # Confirm overwrite if file exists
    if os.path.exists(path):
        resp = input(f"\n⚠️ Config file already exists at:\n{path}\nDo you want to overwrite it? (y/N): ").strip().lower()
        if resp != "y":
            print("❌ Aborting. Configuration not changed.")
            return

    if interactive:
        print("\n🔧 Interactive setup mode. Please fill in all configuration fields.\n")

        def ask(prompt):
            val = ""
            while not val:
                val = input(f"{prompt}: ").strip()
            return val

        # Shared SSH values
        ssh_server_ip = ask("SSH Server IP")
        ssh_port = int(ask("SSH Port"))
        ssh_username = ask("SSH Username")
        ssh_password = ask("SSH Password")
        ssh_root_password = ask("Root Password")

        config = {
            "SSH_CONFIG": {
                "server_ip": ssh_server_ip,
                "port": ssh_port,
                "username": ssh_username,
                "password": ssh_password,
                "root_password": ssh_root_password
            },
            "BEELINE_CONFIG": {
                "env_path": ask("Environment script path"),
                "keytab_path": ask("Kerberos keytab path"),
                "user": ask("Kerberos user"),
                "beeline_path": ask("Beeline path"),
                "DEFAULT_QUEUE": ask("Default queue name")
            },
            "WINSCP_CONFIG": {
                "winscp_path": ask("WinSCP executable path"),
                "server_address": ssh_server_ip,  # Reuse SSH values
                "username": ssh_username,
                "password": ssh_password,
                "remote_path": ask("Remote path for uploads"),
                "hostkey": ask("SSH host key fingerprint"),
                "export_dir": ask("Export directory path")
            }
        }

    else:
        print("\n🛠️ Creating config with placeholder values...")
        config = {
            "SSH_CONFIG": {
                "server_ip": "e.g., 10.0.0.1",
                "port": 22,
                "username": "your_ssh_username",
                "password": "your_ssh_password",
                "root_password": "your_root_password"
            },
            "BEELINE_CONFIG": {
                "env_path": "/path/to/env_script",
                "keytab_path": "/path/to/your.keytab",
                "user": "kerberos_user",
                "beeline_path": "/path/to/beeline",
                "DEFAULT_QUEUE": "your_default_queue"
            },
            "WINSCP_CONFIG": {
                "winscp_path": "C:\\Path\\To\\WinSCP.exe",
                "server_address": "same_as_ssh_server_ip",
                "username": "same_as_ssh_username",
                "password": "same_as_ssh_password",
                "remote_path": "/remote/upload/path",
                "hostkey": "host key fingerprint here",
                "export_dir": "/export/path"
            }
        }

    # Save the config file
    with open(path, "w") as f:
        json.dump(config, f, indent=4)

    print(f"✅ Configuration saved to:\n{path}")
    print("You can manually edit this file later to update any values.")

    # Attempt to open the config file
    try:
        if platform.system() == "Windows":
            os.startfile(path)
        elif platform.system() == "Darwin":
            subprocess.call(["open", path])
        else:
            subprocess.call(["xdg-open", path])
    except Exception as e:
        print(f"\n⚠️ Could not open config file automatically: {e}")
        print(f"Please open it manually at: {path}")



from typing import Any
from datetime import datetime

_all_ = ['convert_month','convert_day','clean_output']

def convert_month(value, offset=0):
    """
    Convert between this_month (months since Jan 1970) and YYYYMM.
    Optional offset (months) can be added or subtracted.
    """
    if isinstance(value, str) and len(value) == 6:
        year = int(value[:4])
        month = int(value[4:])
        this_month = (year - 1970) * 12 + (month - 1)
        return this_month + offset
    
    elif isinstance(value, int):
        value += offset
        year = 1970 + value // 12
        month = (value % 12) + 1
        return f"{year}{month:02d}"
    
    else:
        raise ValueError("Input must be either YYYYMM string or integer months since Jan 1970")


from datetime import datetime, timedelta

def convert_day(value, offset=0):
    """
    Convert between days since epoch and YYYYMMDD.
    Optional offset (days) can be added or subtracted.
    """
    epoch = datetime(1970, 1, 1)
    
    if isinstance(value, int):
        date = epoch + timedelta(days=value + offset)
        return date.strftime("%Y%m%d")
    
    elif isinstance(value, str) and len(value) == 8:
        date = datetime.strptime(value, "%Y%m%d")
        days_since_epoch = (date - epoch).days
        return days_since_epoch + offset
    
    else:
        raise ValueError("Input must be either integer days since epoch or YYYYMMDD string")


def clean_out(
    output: str,
    *,
    insert_zwsp: bool = True,
    header_rule: str = "-",
    header_align: str = "center",   # "center" or "left"
    extra_right_pad: int = 1        # add a small extra space on the right of every column
) -> str:
    """
    Rebuild a clean ASCII table from Beeline output quickly.
    Optimized for large tables with vectorized operations.
    - Center-aligns header (configurable), left-aligns data
    - Adds extra right padding per column (configurable)
    - Optional zero-width space before right padding so double-click stops at cell end
    """
    ZWSP = "\u200b"

    # 1) Fast filtering of table rows using list comprehension
    lines = [ln for ln in output.splitlines() if ln.lstrip().startswith("|")]
    if not lines:
        return ""

    # 2) Optimized row parsing with pre-allocated lists
    rows = []
    for ln in lines:
        # Split and strip in one pass, avoid double iteration
        cells = ln.split("|")
        if len(cells) >= 3:  # At least |cell|cell|
            rows.append([cell.strip() for cell in cells[1:-1]])
    
    if not rows:
        return ""
    
    headers, data = rows[0], rows[1:]
    cols = len(headers)

    # 3) Vectorized width calculation - much faster for large datasets
    if data:
        # Convert to numpy arrays for vectorized operations (if available)
        try:
            import numpy as np
            # Create array of cell lengths for all data rows
            data_lengths = np.array([[len(cell) for cell in row] for row in data])
            # Get max length per column
            data_max_widths = np.max(data_lengths, axis=0)
            # Compare with header lengths
            header_lengths = np.array([len(cell) for cell in headers])
            widths = np.maximum(data_max_widths, header_lengths).tolist()
        except ImportError:
            # Fallback to optimized Python approach
            widths = [len(cell) for cell in headers]
            for row in data:
                for i, cell in enumerate(row):
                    if len(cell) > widths[i]:
                        widths[i] = len(cell)
    else:
        widths = [len(cell) for cell in headers]

    # 4) Pre-calculate padding values to avoid repeated calculations
    extra_pad = max(0, extra_right_pad)
    is_center = header_align.lower() == "center"
    
    # Pre-calculate separator strings
    sep_chars = ["-" * (widths[i] + 2 + extra_pad) for i in range(cols)]
    separator_str = "+" + "+".join(sep_chars) + "+"
    header_sep_str = "+" + "+".join(header_rule * (widths[i] + 2 + extra_pad) for i in range(cols)) + "+"

    # 5) Optimized row formatting with pre-calculated values
    def format_row_fast(cells, is_header=False):
        if len(cells) != cols:
            return None
            
        parts = ["|"]
        for i, cell in enumerate(cells):
            cell_len = len(cell)
            base_pad = widths[i] - cell_len
            
            if is_header and is_center:
                left_inner = base_pad // 2
                right_inner = base_pad - left_inner + extra_pad
                # Build piece efficiently
                piece = f" {' ' * left_inner}{cell}"
                if insert_zwsp and right_inner > 0 and cell:
                    piece += ZWSP
                piece += f"{' ' * right_inner} |"
            else:
                right_pad = base_pad + extra_pad
                piece = f" {cell}"
                if insert_zwsp and right_pad > 0 and cell:
                    piece += ZWSP
                piece += f"{' ' * right_pad} |"
            parts.append(piece)
        return "".join(parts)

    # 6) Build output efficiently using list comprehension
    out_lines = [
        separator_str,
        format_row_fast(headers, is_header=True),
        header_sep_str,
    ]
    
    # Add data rows efficiently
    out_lines.extend(format_row_fast(row, is_header=False) for row in data)
    out_lines.append(separator_str)

    return "\n".join(out_lines)

