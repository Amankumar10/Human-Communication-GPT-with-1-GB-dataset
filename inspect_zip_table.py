import sqlite3

db_path = "data/OpenSubtitles/eng_subtitles_database.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Show columns of the zipfiles table
cursor.execute("PRAGMA table_info(zipfiles);")
columns = cursor.fetchall()

print("📋 Columns in 'zipfiles':")
for c in columns:
    print(f" - {c[1]} ({c[2]})")

# Peek a sample row
print("\n🔍 Preview 1 row:")
cursor.execute("SELECT * FROM zipfiles LIMIT 1;")
row = cursor.fetchone()
if row:
    print(f"Row length: {len(row)} columns")
    for i, val in enumerate(row):
        if isinstance(val, bytes):
            print(f"[{i}] BLOB data ({len(val)} bytes)")
        else:
            print(f"[{i}] {val}")
else:
    print("⚠️ No rows found in zipfiles")

conn.close()
