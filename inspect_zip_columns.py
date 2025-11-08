import sqlite3

db_path = "data/OpenSubtitles/eng_subtitles_database.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(zipfiles);")
cols = cursor.fetchall()

print("📋 Columns in zipfiles table:")
for c in cols:
    print(f" - {c[1]} ({c[2]})")

conn.close()
