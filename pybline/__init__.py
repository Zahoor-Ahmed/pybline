"""
pybline: A modular Python library for executing SQL queries over Beeline, managing remote file operations,
and handling data transformation tasks in big data environments.

This package provides high-level utilities for:
- Establishing SSH connections and running shell commands.
- Running SQL queries using a Beeline session over SSH.
- Uploading/downloading files and tables between local and remote systems.
- Data export, formatting, and time-based partition utilities.
- Integration with Jupyter via SQL magic commands.
"""

# Import public API from submodules

from .ssh import ssh_connection, run_shell, run_shell_blocking
from .core import beeline_session, run_sql, run_pgsql
from .fileops import upload_file, download_file, df_to_Table, table_to_df, download_df
from .utils import alert, text_to_df, pgsql_to_df, to_sql_inlist, todayx, this_monthx, export, daypartitions, daypartitions_to_sec, set_env, convert_day, convert_month, clean_out, df2postgres, postgres2df
# from .meta import confirm_table_size
try:
    from .ipython import register_sql_magic
    register_sql_magic()
except:
    pass  # Ignore IPython magic registration in non-IPython environments

_all_ = [
    'ssh_connection', 'run_shell', 'run_shell_blocking',
    'beeline_session', 'run_sql', 'run_pgsql',
    'upload_file', 'download_file', 'df_to_Table', 'table_to_df', 'download_df',
    'alert', 'text_to_df', 'pgsql_to_df', 'to_sql_inlist', 'todayx', 'this_monthx', 'export', 'daypartitions', 'daypartitions_to_sec', 'set_env', 'df2postgres', 'postgres2df',
    'register_sql_magic'
]


#---------  backward compatibility  -------------

# Old names mapped to new functions
execute_sql_query = run_sql
send_linux_command = run_shell
df_to_dbTable = df_to_Table
dbTable_to_csv = table_to_df

# Add aliases to _all_ to support wildcard imports
_all_.extend(['execute_sql_query', 'send_linux_command', 'df_to_dbTable', 'dbTable_to_csv'])

#--------------------------------------------------


# Inject current date values as global constants for convenience
from datetime import datetime
import builtins

# Define today and this_month as global constants (days/months since 1970)
builtins.today = (datetime.now() - datetime(1970, 1, 1)).days
builtins.this_month = (datetime.now().year - 1970) * 12 + datetime.now().month - 1

# These imports are for type checkers like Pylance
from typing import Literal
today: int
this_month: int