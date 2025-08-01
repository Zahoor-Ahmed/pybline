"""
ssh.py

This module provides utility functions for establishing SSH connections and executing
shell commands remotely on a server. It supports persistent shell sessions as well as
single-command execution using sudo privileges.
"""

import re
import time
import paramiko # type: ignore
from .config import SSH_CONFIG


def ssh_connection():
    """
    Establish an SSH connection and return a root-level shell session.

    Configuration is read from the global SSH_CONFIG dictionary, which must include:
    - server_ip: IP address of the remote server.
    - port: SSH port number.
    - username: SSH login username.
    - password: User's SSH password.
    - root_password: Password to elevate to root via 'su'.

    Returns:
        tuple: A tuple (client, shell) where `client` is the SSHClient instance and
               `shell` is an interactive shell session with root access.

    Raises:
        ConnectionError: If the SSH connection or shell elevation fails.
    """
    config = SSH_CONFIG()
    server_ip = config.get("server_ip")
    port = config.get("port")
    username = config.get("username")
    password = config.get("password")
    root_password = config.get("root_password")

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(server_ip, port, username, password)
        client.get_transport().set_keepalive(900)

        shell = client.invoke_shell()
        shell.send('su - root\n')
        time.sleep(0.1)
        shell.send(root_password + '\n')
        time.sleep(0.1)

        return client, shell

    except Exception as e:
        raise ConnectionError(f"SSH connection failed: {e}")


def clean_output(output, command=None):
    """
    function to extract clean shell command output.
    """
    lines = output.strip().split('\n')
    cleaned = []

    skip_phrases = [
        'Last login:', 'Authorized users only.', 'Password:',
        'su - root', '$source', '$kinit', 'paas@', '[root@'
    ]
    if command:
        skip_phrases.append(command.strip())

    for line in lines:
        # Skip prompt lines or echoed commands
        if any(phrase in line for phrase in skip_phrases):
            continue
        cleaned.append(line)

    return '\n'.join(cleaned)


def run_shell(command: str) -> str:
    """
    Executes a shell command on the remote server via SSH and waits until a known marker is received,
    ensuring that even very long commands are fully executed before returning.

    Args:
        command (str): Shell command to execute.

    Returns:
        str: Cleaned output.
    """
    from .config import BEELINE_CONFIG
    from .ssh import ssh_connection
    import time

    config = BEELINE_CONFIG()

    # Add environment setup for Hadoop/HDFS
    if any(kw in command.lower() for kw in ["hdfs", "hadoop"]):
        env_cmd = f"source {config.get('env_path')}"
        kinit_cmd = f"kinit -kt {config.get('keytab_path')} {config.get('user')}"
        command = f"{env_cmd}; {kinit_cmd}; {command}"

    # Append unique marker to detect end of command
    marker = "__CMD_DONE__"
    full_command = f"{command}; echo {marker}"

    ssh_client, shell = ssh_connection()
    shell.settimeout(0.5)  # Allow non-blocking read
    shell.send(full_command + "\n")

    output = ""
    buffer = ""
    max_idle_waits = 600  # Wait up to 5 minutes idle if needed
    idle_counter = 0

    while True:
        try:
            if shell.recv_ready():
                recv = shell.recv(65535).decode("utf-8")
                buffer += recv
                output += recv
                idle_counter = 0

                if marker in buffer:
                    break
            else:
                time.sleep(0.5)
                idle_counter += 1

            if idle_counter > max_idle_waits:
                print("⚠️ Timeout: Command took too long without output.")
                break

        except Exception as e:
            print(f"⚠️ Exception during shell read: {e}")
            break

    ssh_client.close()
    return clean_output(output, command)


def run_shell_blocking(command: str, marker="__COMPLETE__") -> str:
    """
    Executes a shell command and blocks until a marker is seen.
    Use for critical chained shell steps (e.g., writing large files).
    """
    from .config import BEELINE_CONFIG
    from .ssh import ssh_connection
    import time

    config = BEELINE_CONFIG()

    if any(kw in command.lower() for kw in ["hdfs", "hadoop"]):
        env_cmd = f"source {config.get('env_path')}"
        kinit_cmd = f"kinit -kt {config.get('keytab_path')} {config.get('user')}"
        command = f"{env_cmd}; {kinit_cmd}; {command}"

    ssh_client, shell = ssh_connection()
    shell.send(command + "\n")
    time.sleep(1)

    output = ""
    timeout = 600  # Max wait time in seconds (10 min)
    start = time.time()

    while time.time() - start < timeout:
        if shell.recv_ready():
            chunk = shell.recv(65535).decode("utf-8")
            output += chunk
            if marker in chunk:
                break
        else:
            time.sleep(0.5)

    ssh_client.close()

    if marker not in output:
        raise RuntimeError("Shell command may not have completed fully.")

    return clean_output(output, command)
