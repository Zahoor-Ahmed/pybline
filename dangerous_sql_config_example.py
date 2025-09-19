#!/usr/bin/env python3
"""
Example showing how to configure dangerous SQL operations detection.

To enable/disable specific operations, edit the is_dangerous_sql() function in pybline/utils.py
and comment/uncomment the lines in the dangerous_keywords and dangerous_patterns lists.
"""

import pybline as pb

print("Dangerous SQL Operations Configuration Example")
print("=" * 50)
print()

print("Currently ENABLED dangerous operations:")
print("- DROP (DROP TABLE, DROP DATABASE, etc.)")
print("- DELETE (DELETE FROM table)")
print("- TRUNCATE (TRUNCATE TABLE)")
print("- GRANT (GRANT permissions)")
print("- REVOKE (REVOKE permissions)")
print("- EXECUTE/EXEC (Execute stored procedures)")
print()

print("Currently DISABLED (commented out) operations:")
print("- ALTER (ALTER TABLE - often safe for schema changes)")
print("- UPDATE (UPDATE table SET - often needed for data updates)")
print("- INSERT (INSERT INTO - often needed for data insertion)")
print("- CREATE (CREATE TABLE - often safe for new objects)")
print()

print("To modify which operations are considered dangerous:")
print("1. Open pybline/utils.py")
print("2. Find the is_dangerous_sql() function")
print("3. Comment/uncomment lines in the dangerous_keywords list")
print("4. Comment/uncomment lines in the dangerous_patterns list")
print()

print("Example - to enable ALTER TABLE detection:")
print("Change: # 'ALTER',        # ALTER TABLE - COMMENTED OUT")
print("To:     'ALTER',          # ALTER TABLE")
print()

print("Example - to disable DROP detection:")
print("Change: 'DROP',           # DROP TABLE, DROP DATABASE, etc.")
print("To:     # 'DROP',         # DROP TABLE, DROP DATABASE, etc.")
print()

print("Testing current configuration:")
print("-" * 30)

# Test a safe query (should not trigger dialog)
print("1. Safe query (SELECT):")
try:
    result, rows = pb.run_sql("SELECT 1 as test")
    print(f"   Result: {rows}")
except Exception as e:
    print(f"   Error: {e}")

print()

# Test a dangerous query (should trigger dialog)
print("2. Dangerous query (DROP):")
print("   This will show a confirmation dialog...")
try:
    result, rows = pb.run_sql("DROP TABLE nonexistent_table")
    print(f"   Result: {rows}")
except Exception as e:
    print(f"   Error: {e}")

print()
print("Configuration complete! Modify pybline/utils.py to customize dangerous operations.")
