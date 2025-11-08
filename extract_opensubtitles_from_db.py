import sqlite3, zipfile, io, os
from tqdm import tqdm

db_path = "data/OpenSubtitles/eng_subtitles_database.db"
output_path = "data/opensubtitles_extracted/"
output_txt = "data/opensubtitles_en.txt"

os.makedirs(output_path, exist_ok=True)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Count rows
cursor.execute("SELECT COUNT(*) FROM zipfiles;")
total = cursor.fetchone()[0]
print(f"🗂️ Found {total:,} ZIP files inside the database")

# Open output file once, write line by line
with open(output_txt, "w", encoding="utf-8") as out_f:
    cursor.execute("SELECT name, content FROM zipfiles;")  # use 'content' column
    for name, blob in tqdm(cursor.fetchall(), desc="Extracting ZIPs"):
        try:
            zip_bytes = io.BytesIO(blob)
            with zipfile.ZipFile(zip_bytes) as zf:
                for member in zf.namelist():
                    if member.endswith((".srt", ".txt")):
                        with zf.open(member) as f:
                            text = f.read().decode("utf-8", errors="ignore")
                            text = text.replace("\r", "").strip()
                            # Stream each line directly to disk
                            for line in text.split("\n"):
                                line = line.strip()
                                if not line or "-->" in line or line.isdigit():
                                    continue
                                out_f.write(line + " __eou__\n")
        except Exception as e:
            print(f"⚠️ Error processing {name}: {e}")

conn.close()
print(f"✅ Finished extracting → {output_txt}")
