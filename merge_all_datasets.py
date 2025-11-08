#!/usr/bin/env python3
"""
merge_all_datasets.py
-------------------------------------
✅ Combines & formats:
   - DailyDialog (.txt)
   - EmpatheticDialogues (.csv)
   - PersonaChat (.csv)
   - OpenSubtitles (.txt)

🎯 Output:
   data/human_chat_1gb_readable.txt

🧠 Features:
- Cleans punctuation & symbols
- Alternates User1/User2 roles
- Adds random "help me" prompts
- Works with both TXT and CSV files
-------------------------------------
"""

import os
import re
import csv
import random
from tqdm import tqdm

# ---------- INPUT FILES ----------
datasets = {
    "DailyDialog": "data/dailydialog-parquet/train/dialogues_train.txt",
    "EmpatheticDialogues": "data/empathetic_dialogues/train.csv",
    "PersonaChat": "data/PersonaChat/personality.csv",
    "OpenSubtitles": "data/OpenSubtitles/opensubtitles_en.txt",
}

output_path = "data/human_chat_1gb_readable.txt"

# ---------- RANDOM PROMPTS ----------
PROMPTS = [
    "Please help me communicate better.",
    "How should I deal with this situation?",
    "How can I understand this person better?",
    "Am I right or wrong here?",
    "What’s a calm way to reply?",
    "What could I say next?",
    "Help me see this from their perspective.",
    "What’s the best way to handle this conversation?",
    "How can I express myself more kindly?",
    "What would be a good way to respond?",
    "How do I de-escalate this calmly?",
]

# ---------- CLEANING ----------
def clean_text(text):
    text = str(text)
    text = text.replace(" ,", ",").replace(" .", ".")
    text = text.replace(" ?", "?").replace(" !", "!")
    text = text.replace(" ’", "'").replace("‘", "'")
    text = text.replace("“", '"').replace("”", '"')
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    text = re.sub(r"[^\x00-\x7F]+", "", text)  # remove weird chars
    return text


# ---------- CONVERSION ----------
def convert_dialogue(raw_dialogue):
    """Converts a dialogue (string) into readable chat format."""
    sentences = [s.strip() for s in raw_dialogue.split("__eou__") if s.strip()]
    if len(sentences) < 2:
        return None

    lines = []
    for i, sent in enumerate(sentences):
        speaker = "User1" if i % 2 == 0 else "User2"
        lines.append(f"{speaker}: {clean_text(sent)}")

    lines.append(random.choice(PROMPTS))
    lines.append("Bot:")
    return "\n".join(lines)


# ---------- DATASET READING ----------
def read_txt_dataset(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read().split("\n")


def read_csv_dataset(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            joined = " ".join(row)
            rows.append(joined)
    return rows


# ---------- MERGING ----------
def merge_datasets(datasets):
    all_dialogs = []
    seen = set()

    for name, path in datasets.items():
        if not os.path.exists(path):
            print(f"⚠️ Missing dataset: {name} → {path}")
            continue

        print(f"📂 Loading {name} ...")

        # Load according to file type
        if path.endswith(".csv"):
            raw_data = read_csv_dataset(path)
        elif path.endswith(".txt"):
            raw_data = read_txt_dataset(path)
        else:
            print(f"⚠️ Unsupported file type: {path}")
            continue

        count_before = len(all_dialogs)
        for line in tqdm(raw_data, desc=f"Processing {name}"):
            if "__eou__" not in line:
                continue
            converted = convert_dialogue(line)
            if not converted or len(converted) < 20:
                continue
            if converted not in seen:
                seen.add(converted)
                all_dialogs.append(converted)

        print(f"✅ Added {len(all_dialogs) - count_before:,} dialogues from {name}")

    return all_dialogs


# ---------- MAIN ----------
if __name__ == "__main__":
    all_dialogs = merge_datasets(datasets)

    print(f"\n🔀 Shuffling {len(all_dialogs):,} dialogues for randomness...")
    random.shuffle(all_dialogs)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(all_dialogs))

    print("\n✅ FINAL STATS")
    print(f"Total dialogues merged: {len(all_dialogs):,}")
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"File saved → {output_path} ({size_mb:.2f} MB)")
    print("🎯 Dataset ready for tokenizer training!")
