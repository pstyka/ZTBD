import subprocess

print("=== PostgreSQL benchmark: INSERT ===")
subprocess.run(["python", "insert_postgresql.py"])

print("\n=== PostgreSQL benchmark: UPDATE ===")
subprocess.run(["python", "update_postgresql.py"])

print("\n=== PostgreSQL benchmark: DELETE ===")
subprocess.run(["python", "delete_postgresql.py"])

print("\n=== PostgreSQL benchmark: SELECT ===")
subprocess.run(["python", "select_postgresql.py"])

print("\n✅ All PostgreSQL operations completed.")
