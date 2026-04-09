import whisper
from pathlib import Path

audio_path = Path("/Users/ozgur/Downloads/video.mp3")

model = whisper.load_model("base")
result = model.transcribe(str(audio_path), language="tr")

output_txt = Path("test_transcript.txt")

with open(output_txt, "w", encoding="utf-8") as f:
    f.write(result["text"])

print("✅ Transcription tamamlandı")
print("📄 Çıktı:", output_txt.resolve())

