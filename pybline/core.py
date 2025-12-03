import os
import re
import time
import datetime
from pathlib import Path

import pandas as pd  # type: ignore
import psycopg2  # type: ignore

from .ssh import ssh_connection
from .utils import clean_sql, alert as play_alert_sound, clean_out, is_dangerous_sql, show_sql_confirmation_dialog
from .config import BEELINE_CONFIG, POSTGRES_CONFIG

def beeline_session(shell, queue_name=None, timeout=10):
    config = BEELINE_CONFIG()
    env_path = config.get("env_path", "")
    keytab_path = config.get("keytab_path", "")
    user = config.get("user", "")
    beeline_path = config.get("beeline_path", "")

    if queue_name:
        beeline_command = f"source {env_path}; kinit -kt {keytab_path} {user}; {beeline_path} {queue_name};"
    else:
        beeline_command = f"source {env_path}; kinit -kt {keytab_path} {user}; {beeline_path};"

    shell.send(beeline_command + "\n")
    output = ''
    start_time = time.time()
    while True:
        if shell.recv_ready():
            new_data = shell.recv(65535).decode('utf-8')
            output += new_data
            if "Connecting to jdbc:fiber" in new_data:
                break
        if time.time() - start_time > timeout:
            break
        time.sleep(0.1)


def extract_query_output(output, sql_query=None):
    output = output.replace('\r\n', '\n')

    # Step 1: Remove pre-query logs up to the last JDBC prompt
    prompt_marker = re.search(r"(?m)^0: jdbc:[^\n]+>", output)
    if prompt_marker:
        output = output[prompt_marker.end():].strip()

    lines = output.splitlines()

    # Step 2: Detect valid result table with +--- border, | header, +--- border
    table_start_idx = None
    table_end_idx = None
    for i in range(len(lines) - 2):
        if lines[i].strip().startswith("+") and "-" in lines[i] and \
           lines[i+1].strip().startswith("|") and \
           lines[i+2].strip().startswith("+") and "-" in lines[i+2]:
            table_start_idx = i
            for j in range(len(lines) - 1, i, -1):
                if lines[j].strip().startswith("+") and "-" in lines[j]:
                    table_end_idx = j
                    break
            break

    # If valid table detected, extract it
    if table_start_idx is not None and table_end_idx is not None and table_end_idx > table_start_idx:
        table_output = "\n".join(lines[table_start_idx:table_end_idx + 1])
        rows_line = ""
        for i in range(table_end_idx + 1, len(lines)):
            if re.match(r"^\d+\srows selected|No rows selected|1 row selected", lines[i].strip()):
                rows_line = lines[i].strip()
                break
        table_output = clean_out(table_output)
        return table_output.strip(), rows_line

    # Step 3: Extract error message cleanly starting from "Error:" and stopping before "== Carbon Parser:"
    error_index = output.find("Error:")
    if error_index != -1:
        cleaned = output[error_index:]
        cut_off = cleaned.find("== Carbon Parser:")
        if cut_off != -1:
            cleaned = cleaned[:cut_off]
        return cleaned.strip(), ""

    # Step 4: Return remaining output if nothing matches
    return output.strip(), ""


def run_sql(sql_query, queue_name=None, io=True, timeout=0, log_enabled=True, warn=True, alert=True):
    if queue_name is None:
        queue_name = BEELINE_CONFIG().get("DEFAULT_QUEUE", "")

    # Check for dangerous SQL operations (controllable via warn flag)
    if warn and is_dangerous_sql(sql_query):
        if not show_sql_confirmation_dialog(sql_query):
            if io:
                print("SQL operation cancelled by user.")
            return "", "Operation cancelled"

    ssh_client, shell = ssh_connection()
    beeline_session(shell, queue_name)

    sql_query = clean_sql(sql_query)
    while shell.recv_ready():
        shell.recv(65535)
    shell.send(sql_query + "\n;\n")

    output = ''
    start_time = time.time()
    progress_printed = False
    last_row_count = 0
    seen_rows = set()  # Track unique rows across all iterations
    
    while True:
        if timeout > 0 and time.time() - start_time > timeout:
            print("Timeout reached, exiting loop.")
            if alert:
                try:
                    play_alert_sound()
                except Exception:
                    pass
            break
        if shell.recv_ready():
            new_data = shell.recv(65535).decode('utf-8')
            output += new_data
            
            # Count actual data rows for progress tracking
            if io:  # Only show progress if io is enabled
                lines = output.split('\n')
                data_row_count = 0

                def is_dashed_border(s: str) -> bool:
                    s = s.strip()
                    return s.startswith('+') and '-' in s

                for i in range(len(lines)):
                    line = lines[i].strip()

                    # Skip dashed border lines
                    if is_dashed_border(line):
                        continue

                    # Count only table rows starting with '|'
                    if line.startswith('|'):
                        prev_line = lines[i - 1].strip() if i > 0 else ''
                        next_line = lines[i + 1].strip() if i + 1 < len(lines) else ''

                        # Skip header lines which are surrounded by dashed borders
                        if is_dashed_border(prev_line) and is_dashed_border(next_line):
                            continue

                        data_row_count += 1
                
                # Show progress
                print(f"\rTotal rows fetched = {data_row_count}", end='', flush=True)
                progress_printed = True
            
            if any(x in new_data for x in ["rows selected", "No rows selected", "row selected", "Error"]):
                if alert:
                    try:
                        play_alert_sound()
                    except Exception:
                        pass
                break
        else:
            time.sleep(0.001)
    
    # Clear the progress line if it was printed
    if progress_printed and io:
        # Add delay to show the final row count before clearing
        time.sleep(1.5)
        print("\r" + " " * 50 + "\r", end='', flush=True)

    query_output, rows = extract_query_output(output)

    log_file_path = None
    if log_enabled:
        logs_dir = os.path.normpath(os.path.expanduser("~/pb_logs"))
        current_year = datetime.datetime.now().strftime("%Y")
        current_month = datetime.datetime.now().strftime("%m")
        year_dir = os.path.join(logs_dir, current_year)
        month_dir = os.path.join(year_dir, current_month)
        os.makedirs(month_dir, exist_ok=True)
        today_date = datetime.datetime.now().strftime("%Y_%m_%d")
        log_file_path = os.path.join(month_dir, f"logs_{today_date}.txt")

        with open(log_file_path, "a", encoding="utf-8") as log_file:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write("\n\n" + "-" * 70 + "\n")
            log_file.write(f"{timestamp}\n")
            log_file.write("-" * 70 + "\n")
            log_file.write("\n" + sql_query + "\n\n")
            log_file.write(query_output + "\n" + rows + "\n")

    if "Error" in output and log_file_path:
        file_name_only = os.path.basename(log_file_path)
        error_message_html = f"""
        <p>An error occurred, click to see the logs: 
        <a href='{log_file_path}' target='_blank'>{file_name_only}</a></p>
        """

    if io:
        print(query_output)
        if rows:
            print(rows)

    ssh_client.close()
    time.sleep(0.5)

    return query_output, rows


def run_pgsql(sql_query, io=True, timeout=0, log_enabled=True, warn=True, alert=True):
    """
    Run a PostgreSQL query on the local machine.

    This is similar to run_sql() but targets a local PostgreSQL database instead
    of a remote Hive/Beeline session over SSH.

    Args:
        sql_query (str): SQL query to execute.
        io (bool): If True, prints the formatted output and row count.
        timeout (int): Optional timeout in seconds for the query (0 = no timeout).
        log_enabled (bool): If True, appends query and output to ~/pb_logs (same as run_sql).
        warn (bool): If True, shows a confirmation dialog for dangerous SQL operations.
        alert (bool): If True, plays a sound on completion or error.

    Returns:
        (output, rows): A tuple of formatted output string and a row-count string.
    """

    # Check for dangerous SQL operations (same logic as run_sql)
    if warn and is_dangerous_sql(sql_query):
        if not show_sql_confirmation_dialog(sql_query):
            if io:
                print("SQL operation cancelled by user.")
            return "", "Operation cancelled"

    # Load PostgreSQL connection details from config with sensible defaults
    cfg = {}
    try:
        cfg = POSTGRES_CONFIG() or {}
    except Exception:
        # If config is missing or unreadable, continue with defaults and empty password
        cfg = {}

    host = cfg.get("host", "localhost")
    port = cfg.get("port", 5432)
    dbname = cfg.get("db", cfg.get("database", "streamlit_db"))
    user = cfg.get("user", "postgres")
    password = cfg.get("password", "")

    if not password:
        if io:
            print("❌ PostgreSQL password not found in configuration (POSTGRES_CONFIG).")
            print("   Please update ~/.pybline_config.json or run pybline.set_env().")
        return "", "Missing PostgreSQL password"

    conn = None
    cursor = None
    output_str = ""
    rows_str = ""

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        cursor = conn.cursor()

        # Apply statement timeout if requested (milliseconds)
        if timeout and timeout > 0:
            try:
                cursor.execute(f"SET statement_timeout = {int(timeout * 1000)};")
            except Exception:
                # If setting timeout fails, continue without it
                conn.rollback()
                cursor = conn.cursor()

        # Execute the query
        cursor.execute(sql_query)

        # If the query returns rows (SELECT, etc.), fetch them
        if cursor.description is not None:
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=col_names)
            row_count = len(df)

            if row_count == 0:
                rows_str = "No rows selected"
                output_str = ""
            else:
                # Format DataFrame as a simple text table
                output_str = df.to_string(index=False)
                if row_count == 1:
                    rows_str = "1 row selected"
                else:
                    rows_str = f"{row_count} rows selected"
        else:
            # Non-SELECT statements (INSERT/UPDATE/DELETE, DDL)
            conn.commit()
            affected = cursor.rowcount
            output_str = ""
            if affected == -1:
                rows_str = "Query executed successfully"
            elif affected == 1:
                rows_str = "1 row affected"
            else:
                rows_str = f"{affected} rows affected"

        # Logging (same directory structure as run_sql)
        if log_enabled:
            logs_dir = os.path.normpath(os.path.expanduser("~/pb_logs"))
            current_year = datetime.datetime.now().strftime("%Y")
            current_month = datetime.datetime.now().strftime("%m")
            year_dir = os.path.join(logs_dir, current_year)
            month_dir = os.path.join(year_dir, current_month)
            os.makedirs(month_dir, exist_ok=True)
            today_date = datetime.datetime.now().strftime("%Y_%m_%d")
            log_file_path = os.path.join(month_dir, f"pg_logs_{today_date}.txt")

            with open(log_file_path, "a", encoding="utf-8") as log_file:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_file.write("\n\n" + "-" * 70 + "\n")
                log_file.write(f"{timestamp} [PostgreSQL]\n")
                log_file.write("-" * 70 + "\n")
                log_file.write("\n" + sql_query + "\n\n")
                if output_str:
                    log_file.write(output_str + "\n")
                if rows_str:
                    log_file.write(rows_str + "\n")

        # Optional alert sound
        if alert:
            try:
                play_alert_sound()
            except Exception:
                pass

        # Print to console if requested
        if io:
            if output_str:
                print(output_str)
            if rows_str:
                print(rows_str)

        return output_str, rows_str

    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass

        error_msg = f"Error: {e}"
        if io:
            print(error_msg)

        if alert:
            try:
                play_alert_sound()
            except Exception:
                pass

        # Log error if logging enabled
        if log_enabled:
            logs_dir = os.path.normpath(os.path.expanduser("~/pb_logs"))
            current_year = datetime.datetime.now().strftime("%Y")
            current_month = datetime.datetime.now().strftime("%m")
            year_dir = os.path.join(logs_dir, current_year)
            month_dir = os.path.join(year_dir, current_month)
            os.makedirs(month_dir, exist_ok=True)
            today_date = datetime.datetime.now().strftime("%Y_%m_%d")
            log_file_path = os.path.join(month_dir, f"pg_logs_{today_date}.txt")

            try:
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    log_file.write("\n\n" + "-" * 70 + "\n")
                    log_file.write(f"{timestamp} [PostgreSQL ERROR]\n")
                    log_file.write("-" * 70 + "\n")
                    log_file.write("\n" + sql_query + "\n\n")
                    log_file.write(error_msg + "\n")
            except Exception:
                pass

        return "", ""

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

