#!/usr/bin/env python3
"""
make_human_chat_dataset.py
-------------------------------------
Builds the complete dataset for Human-Communication-GPT
by merging:
 - DailyDialog
 - EmpatheticDialogues
 - PersonaChat
 - OpenSubtitles

🎯 Output: data/human_chat_1gb_readable.txt
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

# ---------- PROMPTS ----------
PROMPTS = [
    # 🌱 Communication & Clarity
    "Please help me communicate better.",
    "How should I deal with this situation?",
    "How can I make my message clearer?",
    "What’s a calm way to reply?",
    "How can I explain my feelings without causing tension?",
    "What could I say next?",
    "How do I express disagreement respectfully?",
    "What’s the best way to start this conversation?",
    "How can I communicate my needs better?",
    "How do I stop sounding defensive?",
    "How can I listen more actively?",
    "How do I apologize sincerely?",
    "What’s a kind way to say no?",
    "How can I express myself more confidently?",
    "How do I handle silence in conversations?",
    "How can I talk about my emotions clearly?",
    "How can I show appreciation through words?",
    "What’s a better way to express frustration?",
    "How do I ask for help without feeling weak?",
    "How can I comfort someone who’s upset?",

    # 💞 Empathy & Understanding
    "How can I understand this person better?",
    "Help me see this from their perspective.",
    "Why do people misunderstand each other?",
    "How do I become more empathetic?",
    "Why is empathy important?",
    "What does it mean to really listen?",
    "How can I show that I care?",
    "How do I make someone feel heard?",
    "How can I validate another person’s feelings?",
    "What’s the difference between empathy and sympathy?",
    "How can I handle someone else’s emotions gently?",
    "How do I stay calm when someone is angry?",
    "How can I express compassion in words?",
    "How can I connect with someone emotionally?",
    "How do I handle people who shut down?",
    "Why do people avoid emotional conversations?",
    "How can I create emotional safety?",
    "What makes someone feel understood?",
    "Why do people fear being vulnerable?",
    "How can I help someone open up?",

    # 💔 Conflict Resolution
    "How do I de-escalate this calmly?",
    "How can I handle arguments more maturely?",
    "What’s the best way to rebuild trust after a fight?",
    "How can I end an argument without resentment?",
    "How do I talk about boundaries without guilt?",
    "How can I stay calm when criticized?",
    "How do I forgive someone who hurt me?",
    "How can I admit my mistakes without shame?",
    "How do I avoid turning disagreements into fights?",
    "What’s a respectful way to give feedback?",
    "How can I disagree without offending?",
    "Why do we argue even when we care?",
    "How can I make peace after a misunderstanding?",
    "How do I talk to someone who keeps interrupting?",
    "How can I resolve tension in a friendship?",
    "How do I talk to someone who avoids conflict?",
    "What’s the healthiest way to express anger?",
    "How do I let go of grudges?",
    "How can I bring closure to a difficult situation?",
    "How do I know when to stop trying?",

    # 💫 Relationships & Connection
    "What is human connection?",
    "How can I feel more connected to others?",
    "Why do people grow distant?",
    "What makes a strong relationship?",
    "Why do relationships fade over time?",
    "How do I rebuild emotional closeness?",
    "What causes emotional distance?",
    "How can I show love through communication?",
    "What makes someone feel valued?",
    "How can I express affection better?",
    "How can I stay connected in long-distance relationships?",
    "How do I support someone without fixing them?",
    "Why do people push others away?",
    "How can I build trust again?",
    "How can I reconnect after a fight?",
    "Why do people withdraw emotionally?",
    "How can I be a better listener to my partner?",
    "What makes a healthy friendship?",
    "Why do people lose interest over time?",
    "How can I communicate in a more loving way?",

    # 🌻 Self-Reflection & Growth
    "Am I right or wrong here?",
    "How can I handle criticism gracefully?",
    "How do I take responsibility without over-blaming myself?",
    "Why do I get defensive?",
    "How can I stay patient during hard conversations?",
    "Why do I overthink what people say?",
    "How do I control my reactions better?",
    "What can I learn from this argument?",
    "Why do I shut down emotionally?",
    "How can I express myself more kindly?",
    "What should I do when I feel unheard?",
    "How can I be more honest with myself?",
    "Why do I fear confrontation?",
    "How can I speak up without sounding harsh?",
    "Why do I avoid emotional topics?",
    "How can I practice self-compassion?",
    "What triggers my frustration?",
    "Why do I crave approval?",
    "How can I express my emotions without guilt?",
    "How can I communicate when I’m anxious?",

    # 🌎 Humanity & Deeper Meaning
    "What does empathy mean?",
    "Why do humans need connection?",
    "What is vulnerability?",
    "Why do people hide their feelings?",
    "How can communication heal relationships?",
    "Why do misunderstandings happen between people?",
    "What does it mean to feel seen?",
    "Why is honesty difficult?",
    "How do emotions affect communication?",
    "What makes conversations meaningful?",
    "Why do we fear being honest?",
    "Why do people stop listening to each other?",
    "What is the role of trust in communication?",
    "Why is emotional awareness important?",
    "What makes communication authentic?",
    "Why do people struggle to apologize?",
    "What does real compassion look like?",
    "How can words build or destroy trust?",
    "What is emotional intelligence?",
    "How do I build deeper connections with people?",
]

# ---------- TEXT CLEANER ----------
def clean_text(text):
    """Normalize punctuation, remove weird chars"""
    text = str(text)
    text = text.replace(" ,", ",").replace(" .", ".")
    text = text.replace(" ?", "?").replace(" !", "!")
    text = text.replace(" ’", "'").replace("‘", "'")
    text = text.replace("“", '"').replace("”", '"')
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    return text

# ---------- DIALOGUE CONVERTER ----------
def convert_dialogue(raw):
    """Convert raw dialogue text into structured format"""
    sentences = [s.strip() for s in raw.split("__eou__") if s.strip()]
    if len(sentences) < 2:
        return None

    lines = []
    for i, sent in enumerate(sentences):
        speaker = "User1" if i % 2 == 0 else "User2"
        lines.append(f"{speaker}: {clean_text(sent)}")

    # Add random reflective/communication prompt
    lines.append(random.choice(PROMPTS))
    lines.append("Bot:")
    return "\n".join(lines)

# ---------- READERS ----------
def read_txt(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read().split("\n")

def read_csv(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            joined = " ".join(row)
            rows.append(joined)
    return rows

# ---------- MAIN MERGER ----------
def merge_datasets(datasets):
    all_dialogs = []
    seen = set()

    for name, path in datasets.items():
        if not os.path.exists(path):
            print(f"⚠️ Missing: {name} → {path}")
            continue

        print(f"📂 Loading {name} ...")
        if path.endswith(".csv"):
            raw_data = read_csv(path)
        else:
            raw_data = read_txt(path)

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

# ---------- RUN ----------
if __name__ == "__main__":
    all_dialogs = merge_datasets(datasets)

    print(f"\n🔀 Shuffling {len(all_dialogs):,} dialogues...")
    random.shuffle(all_dialogs)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(all_dialogs))

    print("\n✅ FINAL STATS")
    print(f"Total dialogues: {len(all_dialogs):,}")
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"File saved → {output_path} ({size_mb:.2f} MB)")
    print("🎯 Your Human-Communication-GPT dataset is ready!")
