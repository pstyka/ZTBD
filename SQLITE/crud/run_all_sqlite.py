import subprocess

print("=== SQLite benchmark: INSERT ===")
subprocess.run(["python", "insert_sqlite.py"])

print("\n=== SQLite benchmark: UPDATE ===")
subprocess.run(["python", "update_sqlite.py"])

print("\n=== SQLite benchmark: DELETE ===")
subprocess.run(["python", "delete_sqlite.py"])

print("\n=== SQLite benchmark: SELECT ===")
subprocess.run(["python", "select_sqlite.py"])

print("\n✅ All SQLite operations completed.")
