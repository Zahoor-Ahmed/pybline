import os
import re
import time
import datetime
from pathlib import Path

from .ssh import ssh_connection
from .utils import clean_sql, alert
from .config import BEELINE_CONFIG

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


def run_sql(sql_query, queue_name=None, io=True, timeout=0, log_enabled=True):
    if queue_name is None:
        queue_name = BEELINE_CONFIG().get("DEFAULT_QUEUE", "")

    ssh_client, shell = ssh_connection()
    beeline_session(shell, queue_name)

    sql_query = clean_sql(sql_query)
    while shell.recv_ready():
        shell.recv(65535)
    shell.send(sql_query + "\n;\n")

    output = ''
    start_time = time.time()
    while True:
        if timeout > 0 and time.time() - start_time > timeout:
            print("Timeout reached, exiting loop.")
            alert()
            break
        if shell.recv_ready():
            new_data = shell.recv(65535).decode('utf-8')
            output += new_data
            if any(x in new_data for x in ["rows selected", "No rows selected", "row selected", "Error"]):
                alert()
                break
        else:
            time.sleep(0.001)

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
