import whisper
import subprocess
import json
import sys
from pathlib import Path

video_url = input("YouTube video linkini yapıştır: ").strip()

BASE_DIR = Path.cwd()
DOWNLOAD_DIR = BASE_DIR / "downloads"
OUTPUT_DIR = BASE_DIR / "transcripts"

DOWNLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

audio_path = DOWNLOAD_DIR / "audio.mp3"

print("🎧 Ses indiriliyor...")

subprocess.run([
    sys.executable,
    "-m",
    "yt_dlp",
    "-x",
    "--audio-format", "mp3",
    "--cookies-from-browser", "safari",
    "-o", str(audio_path),
    video_url
], check=True)

print("🧠 Speech-to-Text başlıyor...")

model = whisper.load_model("base")
result = model.transcribe(str(audio_path), language="tr")

txt_file = OUTPUT_DIR / "transcript.txt"
with open(txt_file, "w", encoding="utf-8") as f:
    f.write(result["text"])

json_file = OUTPUT_DIR / "transcript.json"
with open(json_file, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("✅ Bitti!")
print(f"📄 Metin: {txt_file}")
print(f"🧾 Detaylı JSON: {json_file}")

