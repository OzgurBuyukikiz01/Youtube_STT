#!/usr/bin/env python3
import os
import sys
import json
import time
import pickle
import subprocess
from pathlib import Path
import yt_dlp
import whisper
from openai import OpenAI

# Google Drive API
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io


# Google Drive API Scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def print_header():
    print("=" * 60)
    print(" YouTube JSONL İşleyici (Google Drive Edition)")
    print("=" * 60)


def authenticate_google_drive():
    """Google Drive API Authentication"""
    print("\n🔐 Google Drive'a bağlanılıyor...")
    
    creds = None
    token_path = 'token.pickle'
    credentials_path = 'credentials.json'
    
    # Token var mı kontrol et
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    # Token yoksa veya geçersizse yenile
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(credentials_path):
                print("\n❌ HATA: 'credentials.json' dosyası bulunamadı!")
                print("\n📋 Çözüm:")
                print("1. https://console.cloud.google.com/ adresine git")
                print("2. Yeni proje oluştur")
                print("3. 'Google Drive API'yi etkinleştir")
                print("4. OAuth 2.0 credentials oluştur (Desktop app)")
                print("5. credentials.json dosyasını bu klasöre indir")
                sys.exit(1)
            
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Token'ı kaydet
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    
    print("✅ Google Drive bağlantısı başarılı!")
    return build('drive', 'v3', credentials=creds)


def extract_folder_id_from_url(url):
    """Google Drive URL'den folder ID çıkar"""
    # https://drive.google.com/drive/folders/FOLDER_ID
    if '/folders/' in url:
        return url.split('/folders/')[-1].split('?')[0]
    # Direkt ID verilmişse
    return url.strip()


def list_jsonl_files_in_folder(service, folder_id):
    """Klasördeki tüm JSONL dosyalarını listele"""
    print(f"\n📁 Klasör taranıyor...")
    
    try:
        query = f"'{folder_id}' in parents and name contains '.jsonl' and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name, size, modifiedTime)",
            orderBy="name"
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            print("❌ Klasörde JSONL dosyası bulunamadı!")
            return []
        
        print(f"✅ {len(files)} JSONL dosyası bulundu:")
        for i, file in enumerate(files, 1):
            size_mb = int(file.get('size', 0)) / (1024 * 1024)
            print(f"  {i}. {file['name']} ({size_mb:.2f} MB)")
        
        return files
    
    except Exception as e:
        print(f"❌ Klasör erişim hatası: {e}")
        print("\n💡 İpucu: Klasör linkinin 'Paylaş' ayarlarından")
        print("   'Linke sahip olan herkes' iznini verdiğinizden emin olun.")
        return []


def download_file_from_drive(service, file_id, destination_path):
    """Google Drive'dan dosya indir"""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(destination_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                print(f"   📥 İndiriliyor... {progress}%", end='\r')
        
        print(f"   ✅ İndirildi: {Path(destination_path).name}")
        return destination_path
    
    except Exception as e:
        print(f"   ❌ İndirme hatası: {e}")
        return None


# ============================================================
# MEVCUT FONKSİYONLAR (HİÇBİR DEĞİŞİKLİK YOK)
# ============================================================

def extract_videos_from_jsonl(jsonl_path):
    """GELİŞMİŞ JSON parsing"""
    print("📋 JSONL parse ediliyor...")
    videos = []
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line_num, line in enumerate(lines, 1):
        try:
            data = json.loads(line.strip())
            
            video_id = None
            def find_video_id(obj):
                if isinstance(obj, dict):
                    if 'videoId' in obj and len(str(obj['videoId'])) == 11:
                        return obj['videoId']
                    for k, v in obj.items():
                        if isinstance(v, (dict, list)):
                            result = find_video_id(v)
                            if result:
                                return result
                elif isinstance(obj, list):
                    for item in obj:
                        result = find_video_id(item)
                        if result:
                            return result
                return None
            
            video_id = find_video_id(data)
            
            if video_id:
                title = find_title(data) or f"Video_{line_num}"
                videos.append({
                    'video_id': str(video_id),
                    'title': title[:60],
                    'line_num': line_num,
                    'json_title': title
                })
        except:
            continue
    
    print(f"✓ {len(videos)} video bulundu")
    for i, v in enumerate(videos[:5]):
        print(f"  {i+1}. {v['video_id']} - {v['json_title'][:50]}...")
    return videos


def find_title(data):
    """Başlık bulma"""
    def search_title(obj):
        if isinstance(obj, dict):
            if 'title' in obj:
                if isinstance(obj['title'], dict) and 'runs' in obj['title']:
                    if obj['title']['runs'] and isinstance(obj['title']['runs'], list) and obj['title']['runs']:
                        return obj['title']['runs'][0].get('text', '')
                elif isinstance(obj['title'], dict):
                    return obj['title'].get('simpleText', '')
            
            for k, v in obj.items():
                if isinstance(v, str) and 20 < len(v) < 150:
                    return v[:100]
                if isinstance(v, (dict, list)):
                    result = search_title(v)
                    if result:
                        return result
        elif isinstance(obj, list):
            for item in obj:
                result = search_title(item)
                if result:
                    return result
        return None
    
    title = search_title(data)
    if title:
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title.replace(' ', '_')[:80]
        return safe_title if safe_title else None
    return None


def get_real_youtube_title(video_id):
    """✅ YOUTUBE'DAN GERÇEK BAŞLIĞI AL"""
    print(f"   🔍 Gerçek YouTube başlığı alınıyor...")
    
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://youtube.com/watch?v={video_id}", download=False)
            real_title = info.get('title', f"{video_id}")
            
            safe_real_title = "".join(c for c in real_title if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_real_title = safe_real_title.replace(' ', '_')[:80]
            
            print(f"   ✅ YouTube başlığı: {safe_real_title[:50]}...")
            return safe_real_title or video_id
    except:
        print(f"   ⚠️ YouTube başlığı alınamadı, ID kullanılır")
        return video_id


def create_unique_folder(output_dir, video_id):
    """✅ AYNI İSİM VARSA (1), (2) vb. EKLE"""
    base_folder = Path(output_dir) / video_id
    
    counter = 1
    video_folder = base_folder
    while video_folder.exists():
        video_folder = base_folder.parent / f"{base_folder.name} ({counter})"
        counter += 1
    
    video_folder.mkdir(parents=True, exist_ok=True)
    return str(video_folder)


def download_mp4_to_folder(video_id, video_folder, real_title):
    """🎯 1. REAL_TITLE İLE MP4 İNDİR"""
    mp4_filename = f"{real_title}.mp4"
    mp4_path = os.path.join(video_folder, mp4_filename)
    
    if os.path.exists(mp4_path):
        print(f"   ✅ MP4 zaten var: {mp4_filename}")
        return mp4_path
    
    print(f"   📥 MP4 indiriliyor → {mp4_filename}")
    
    ydl_opts = {
        'format': 'best[ext=mp4]/best[height<=720]/best',
        'outtmpl': mp4_path,
        'merge_output_format': 'mp4',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web'],
                'skip': ['hls', 'dash'],
            }
        },
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://youtube.com/watch?v={video_id}"])
        
        if os.path.exists(mp4_path):
            print(f"   ✅ MP4 kaydedildi: {mp4_filename}")
            return mp4_path
    except Exception as e:
        print(f"   ❌ MP4 hatası: {e}")
    
    return None


def create_mp3_from_mp4(mp4_path, video_folder, real_title):
    """🎯 2. REAL_TITLE.MP3 OLUŞTUR"""
    mp3_filename = f"{real_title}.mp3"
    mp3_path = os.path.join(video_folder, mp3_filename)
    
    if os.path.exists(mp3_path):
        print(f"   ✅ MP3 zaten var: {mp3_filename}")
        return mp3_path
    
    print(f"   🎵 MP3 oluşturuluyor → {mp3_filename}")
    
    cmd = [
        'ffmpeg', '-i', mp4_path,
        '-vn', '-acodec', 'libmp3lame',
        '-q:a', '2', '-y', mp3_path
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"   ✅ MP3 hazır: {mp3_filename}")
        return mp3_path
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"   ❌ MP3 hatası: FFmpeg gerekli")
        return None


def create_wav_from_mp4(mp4_path, video_folder, real_title):
    """🎯 3. REAL_TITLE.WAV OLUŞTUR"""
    wav_filename = f"{real_title}.wav"
    wav_path = os.path.join(video_folder, wav_filename)
    
    if os.path.exists(wav_path):
        print(f"   ✅ WAV zaten var: {wav_filename}")
        return wav_path
    
    print(f"   🔊 WAV hazırlanıyor → {wav_filename}")
    
    cmd = [
        'ffmpeg', '-i', mp4_path,
        '-vn', '-acodec', 'pcm_s16le',
        '-ar', '16000', '-ac', '1',
        '-y', wav_path
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"   ✅ WAV hazır: {wav_filename}")
        return wav_path
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"   ❌ WAV hatası: FFmpeg gerekli")
        return None


def correct_transcript_with_gpt(raw_transcript, api_key):
    """🤖 GPT ile transcript'teki yazım hatalarını düzelt"""
    print(f"   🤖 GPT ile yazım hataları düzeltiliyor...")
    
    try:
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """Sen Türkçe metin düzeltme uzmanısın. 
                    
Görevin: Speech-to-text (Whisper) tarafından oluşturulan Türkçe transcript'lerdeki yazım hatalarını düzeltmek.

ÖNEMLİ KURALLAR:
1. SADECE yazım hatalarını düzelt (yanlış yazılmış kelimeler, isimler, fiiller)
2. Metni KISA veya YENİDEN YAZMAYA ÇALIŞMA - sadece hataları düzelt
3. Cümle yapısını ve anlamını DEĞİŞTİRME
4. Noktalama işaretlerini DÜZELTME - aynen koru
5. Özel isimleri doğru yaz (örn: "Ekrem imamol" → "Ekrem İmamoğlu")
6. Yanlış duyulan fiilleri düzelt (örn: "kürekle yardı" → "kürekle aldı")
7. Hatalı birleşik/ayrı yazılan kelimeleri düzelt (örn: "cumhur başkanı" → "Cumhurbaşkanı")

Çıktı: Sadece düzeltilmiş metni döndür, açıklama ekleme."""
                },
                {
                    "role": "user",
                    "content": f"Bu transcript'teki yazım hatalarını düzelt:\n\n{raw_transcript}"
                }
            ],
            temperature=0.3,
            max_tokens=4000
        )
        
        corrected_text = response.choices[0].message.content.strip()
        print(f"   ✅ GPT düzeltmesi tamamlandı")
        return corrected_text
        
    except Exception as e:
        print(f"   ⚠️ GPT düzeltme hatası: {e}")
        print(f"   ℹ️ Orijinal transcript kullanılacak")
        return raw_transcript


def create_transcript(video_folder, real_title, json_title, wav_path):
    """🎯 4. TRANSCRIPT TXT - HAM + GPT-DÜZELTİLMİŞ"""
    txt_raw = f"{real_title}_transcript_raw.txt"
    txt_corrected = f"{real_title}_transcript.txt"
    
    txt_raw_path = os.path.join(video_folder, txt_raw)
    txt_corrected_path = os.path.join(video_folder, txt_corrected)
    
    if os.path.exists(txt_corrected_path):
        print(f"   ✅ TXT zaten var: {txt_corrected}")
        return txt_corrected_path
    
    print(f"   📝 Whisper transcript → {txt_raw}")
    
    try:
        model = whisper.load_model("base", device="cpu")
        result = model.transcribe(wav_path, language="tr")
        raw_text = result["text"]
        
        with open(txt_raw_path, 'w', encoding='utf-8') as f:
            f.write(raw_text)
        print(f"   ✅ Ham transcript kaydedildi")
        
        api_key = os.getenv('OPENAI_API_KEY')
        
        if not api_key:
            print(f"   ⚠️ OPENAI_API_KEY bulunamadı - GPT düzeltme ATLANACAK")
            with open(txt_corrected_path, 'w', encoding='utf-8') as f:
                f.write(raw_text)
            return txt_corrected_path
        
        corrected_text = correct_transcript_with_gpt(raw_text, api_key)
        
        with open(txt_corrected_path, 'w', encoding='utf-8') as f:
            f.write(corrected_text)
        
        print(f"   ✅ Düzeltilmiş transcript: {txt_corrected}")
        return txt_corrected_path
        
    except Exception as e:
        print(f"   ❌ Transcript hatası: {e}")
        return None


def download_thumbnail(video_id, video_folder, real_title):
    """🖼️ 5. YOUTUBE THUMBNAIL İNDİR"""
    thumb_filename = f"{real_title}_thumbnail.jpg"
    thumb_path = os.path.join(video_folder, thumb_filename)
    
    if os.path.exists(thumb_path):
        print(f"   ✅ Thumbnail zaten var: {thumb_filename}")
        return thumb_path
    
    print(f"   🖼️ Thumbnail indiriliyor → {thumb_filename}")
    
    ydl_opts = {
        'writethumbnail': True,
        'writeinfojson': False,
        'skip_download': True,
        'outtmpl': os.path.join(video_folder, f"%(title)s.%(ext)s"),
        'quiet': True,
        'no_warnings': True,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://youtube.com/watch?v={video_id}"])
        
        for thumb_file in Path(video_folder).glob(f"{real_title}*.jpg"):
            thumb_file.rename(thumb_path)
            break
        else:
            for thumb_file in Path(video_folder).glob("*.*"):
                if thumb_file.suffix.lower() in ['.jpg', '.jpeg', '.png']:
                    thumb_file.rename(thumb_path)
                    break
        
        if os.path.exists(thumb_path):
            print(f"   ✅ Thumbnail kaydedildi: {thumb_filename}")
            return thumb_path
    except Exception as e:
        print(f"   ❌ Thumbnail hatası: {e}")
    
    return None


def process_video(video_info, output_dir):
    """🎯 REAL TITLE İLE İŞLE + GPT-DÜZELTİLMİŞ TRANSCRIPT"""
    video_id = video_info['video_id']
    json_title = video_info.get('json_title', video_id)
    
    print(f"\n🎬 [{video_info['line_num']}] {json_title}")
    
    video_folder = create_unique_folder(output_dir, video_id)
    print(f"   📁 Klasör: {Path(video_folder).name}/")
    
    real_title = get_real_youtube_title(video_id)
    
    print("   1️⃣ MP4...")
    mp4_path = download_mp4_to_folder(video_id, video_folder, real_title)
    if not mp4_path:
        print("   ❌ MP4 yok - ATLANIYOR")
        return None
    
    print("   2️⃣ MP3...")
    mp3_path = create_mp3_from_mp4(mp4_path, video_folder, real_title)
    
    print("   3️⃣ WAV...")
    wav_path = create_wav_from_mp4(mp4_path, video_folder, real_title)
    
    print("   4️⃣ TXT (Whisper + GPT Düzeltme)...")
    txt_path = create_transcript(video_folder, real_title, json_title, wav_path) if wav_path else None
    
    print("   5️⃣ THUMBNAIL...")
    thumb_path = download_thumbnail(video_id, video_folder, real_title)
    
    print(f"   ✅ TAMAMLANDI: {Path(video_folder).name}/")
    return video_folder


# ============================================================
# GOOGLE DRIVE MAIN FUNCTION
# ============================================================

def main():
    clear_screen()
    print_header()
    
    # OpenAI API kontrolü
    if not os.getenv('OPENAI_API_KEY'):
        print("\n⚠️ UYARI: OPENAI_API_KEY bulunamadı!")
        print("GPT düzeltme özelliği çalışmayacak.\n")
        print("Çözüm:")
        print("  export OPENAI_API_KEY='sk-proj-....'")
        print("  # veya")
        print("  setx OPENAI_API_KEY 'sk-proj-....' (Windows)\n")
        
        response = input("Devam etmek istiyor musunuz? (E/H): ").strip().upper()
        if response != 'E':
            return
    
    # Google Drive Authentication
    service = authenticate_google_drive()
    
    # Klasör URL/ID al
    print("\n" + "=" * 60)
    folder_input = input("Google Drive klasör URL'i veya ID: ").strip()
    
    if not folder_input:
        print("❌ Klasör URL'i girilmedi!")
        return
    
    folder_id = extract_folder_id_from_url(folder_input)
    
    # JSONL dosyalarını listele
    jsonl_files = list_jsonl_files_in_folder(service, folder_id)
    
    if not jsonl_files:
        return
    
    # Output dizini
    output_dir = str(Path.home() / "Downloads" / "JSONL_Videos_GoogleDrive")
    os.makedirs(output_dir, exist_ok=True)
    
    # Temp dizini
    temp_dir = Path(output_dir) / "_temp_jsonl"
    temp_dir.mkdir(exist_ok=True)
    
    print(f"\n📁 Çıktı klasörü: {output_dir}")
    print(f"📁 Temp klasör: {temp_dir}")
    
    if input("\nTüm JSONL dosyalarını işle? (E/H): ").strip().upper() != 'E':
        return
    
    # İstatistikler
    total_jsonl = len(jsonl_files)
    total_videos_processed = 0
    total_videos_success = 0
    
    # Her JSONL dosyası için
    for jsonl_index, jsonl_file in enumerate(jsonl_files, 1):
        print("\n" + "=" * 60)
        print(f"📄 JSONL [{jsonl_index}/{total_jsonl}]: {jsonl_file['name']}")
        print("=" * 60)
        
        # Drive'dan indir
        temp_jsonl_path = temp_dir / jsonl_file['name']
        downloaded = download_file_from_drive(service, jsonl_file['id'], str(temp_jsonl_path))
        
        if not downloaded:
            print(f"❌ {jsonl_file['name']} indirilemedi - ATLANIYOR")
            continue
        
        # Videoları parse et
        videos = extract_videos_from_jsonl(str(temp_jsonl_path))
        
        if not videos:
            print(f"❌ {jsonl_file['name']} içinde video yok - ATLANIYOR")
            # Temp dosyayı sil
            temp_jsonl_path.unlink()
            continue
        
        total_videos = len(videos)
        success_count = 0
        
        # Her video için işle
        for video_index, video in enumerate(videos, 1):
            print(f"\n{'─'*50}")
            print(f"🎬 Video [{video_index}/{total_videos}] - JSONL [{jsonl_index}/{total_jsonl}]")
            
            result = process_video(video, output_dir)
            
            if result:
                success_count += 1
                total_videos_success += 1
            
            total_videos_processed += 1
            
            # Rate limit
            if video_index < total_videos:
                print("⏳ 3sn rate limit...")
                time.sleep(3)
        
        print(f"\n✅ {jsonl_file['name']}: {success_count}/{total_videos} video başarılı")
        
        # Temp dosyayı sil
        temp_jsonl_path.unlink()
        
        # Bir sonraki JSONL için bekleme
        if jsonl_index < total_jsonl:
            print(f"\n⏸️ Bir sonraki JSONL için 5 saniye bekleniyor...")
            time.sleep(5)
    
    # Temp klasörü temizle
    try:
        temp_dir.rmdir()
    except:
        pass
    
    # Final rapor
    print("\n" + "=" * 60)
    print("🎉 TÜM JSONL DOSYALARI İŞLENDİ!")
    print("=" * 60)
    print(f"📊 İSTATİSTİKLER:")
    print(f"   📄 Toplam JSONL: {total_jsonl}")
    print(f"   🎬 Toplam Video: {total_videos_processed}")
    print(f"   ✅ Başarılı: {total_videos_success}")
    print(f"   ❌ Başarısız: {total_videos_processed - total_videos_success}")
    print(f"\n📁 Çıktı: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ İptal edildi")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Hata: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)