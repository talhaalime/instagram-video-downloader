
import os
import yt_dlp
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import re
import uuid
from urllib.parse import urlparse
import json
import time
import glob
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional
import threading
from datetime import datetime, timedelta
import tempfile
import shutil
from fastapi.responses import StreamingResponse
from datetime import timedelta

# Pydantic models
class URLRequest(BaseModel):
    url: str
    content_type: Optional[str] = None
    original_url: Optional[str] = None

class DownloadRequest(BaseModel):
    session_id: str
    format_id: str = None

app = FastAPI(title="Instagram Downloader API - High Quality & Auto Cleanup")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup templates and static files
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/images", StaticFiles(directory="images"), name="images")

def sanitize_title(title):
    return re.sub(r'[\\/*?:"<>|]', "_", title)

# Create directories
os.makedirs('static', exist_ok=True)
os.makedirs('templates', exist_ok=True)
os.makedirs('output', exist_ok=True)

# Global variables
video_cache = {}
download_jobs: Dict[str, Dict[str, Any]] = {}
job_lock = threading.Lock()
executor = ThreadPoolExecutor(max_workers=15)

# Cleanup tracking
temp_files = {}
temp_files_lock = threading.Lock()

class InstagramDownloader:
    def __init__(self):
        self.extract_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'extract_flat': False,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'socket_timeout': 30,
            'retries': 3,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        # ✅ OPTIMIZED FOR HIGHEST QUALITY
        self.download_opts = {
            'quiet': True,
            'no_warnings': True,
            'extractaudio': False,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'socket_timeout': 60,
            'retries': 5,  # Increased retries
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            # ✅ FORCE HIGHEST QUALITY FORMATS
            'format_sort': ['res:1080', 'fps', 'hdr:12', 'codec:h264', 'size', 'br', 'asr'],
            'format_sort_force': True,
        }

    def is_valid_instagram_url(self, url):
        """Check if URL is valid Instagram URL"""
        patterns = [
            r'https?://(?:www\.)?instagram\.com/p/[A-Za-z0-9_-]+',
            r'https?://(?:www\.)?instagram\.com/reel/[A-Za-z0-9_-]+',
            r'https?://(?:www\.)?instagram\.com/reels/[A-Za-z0-9_-]+',
            r'https?://(?:www\.)?instagram\.com/tv/[A-Za-z0-9_-]+',
            r'https?://(?:www\.)?instagram\.com/stories/[A-Za-z0-9_.-]+/[0-9]+',
        ]
        
        for pattern in patterns:
            if re.match(pattern, url):
                return True
        return False

    def detect_content_type(self, url):
        """Detect content type from URL"""
        if '/stories/' in url:
            return 'story'
        elif '/reels/' in url or '/reel/' in url:
            return 'reel'
        elif '/p/' in url:
            return 'post'
        elif '/tv/' in url:
            return 'igtv'
        return 'unknown'

    def extract_info(self, url, content_type=None):
        """Extract video information"""
        try:
            # ✅ Handle stories immediately
            if content_type == 'story':
                return {
                    'success': False,
                    'error': 'Instagram Stories are not supported',
                    'error_type': 'story_not_supported',
                    'message': 'Stories cannot be downloaded due to Instagram restrictions'
                }
            
            # Extract info for other content
            with yt_dlp.YoutubeDL(self.extract_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                return {
                    'success': True,
                    'data': info,
                    'content_type': content_type
                }
                
        except yt_dlp.DownloadError as e:
            error_msg = str(e)
            
            if "You need to log in" in error_msg or "cookies" in error_msg.lower():
                return {
                    'success': False,
                    'error': 'This content requires authentication',
                    'error_type': 'authentication_required'
                }
            elif "only available for registered users" in error_msg:
                return {
                    'success': False,
                    'error': 'This content is private',
                    'error_type': 'private_content'
                }
            elif "Video unavailable" in error_msg:
                return {
                    'success': False,
                    'error': 'Video is unavailable',
                    'error_type': 'video_unavailable'
                }
            else:
                return {
                    'success': False,
                    'error': f'Extraction error: {error_msg}',
                    'error_type': 'extraction_error'
                }
        except Exception as e:
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'error_type': 'unexpected_error'
            }

    def download_video_to_temp(self, url, format_id, job_id, content_type=None):
        """Download video to temporary location with auto-cleanup"""
        temp_dir = None
        try:
            if content_type == 'story':
                with job_lock:
                    if job_id in download_jobs:
                        download_jobs[job_id].update({
                            'status': 'failed',
                            'error': 'Stories cannot be downloaded'
                        })
                return None
            
            with job_lock:
                if job_id in download_jobs:
                    download_jobs[job_id]['status'] = 'downloading'
                    download_jobs[job_id]['progress'] = 5

            # ✅ CREATE TEMPORARY DIRECTORY
            temp_dir = tempfile.mkdtemp(prefix='instagram_dl_')
            unique_id = str(uuid.uuid4())[:8]
            download_opts = self.download_opts.copy()
                    
            if format_id == 'audio_only':
                download_opts.update({
                    'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best',
                    'outtmpl': os.path.join(temp_dir, f'{unique_id}_%(title)s.%(ext)s'),
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '320',  # ✅ HIGHEST AUDIO QUALITY
                    }],
                    'prefer_ffmpeg': True,
                })
            elif format_id and format_id != 'best':
                download_opts.update({
                    'format': format_id,
                    'outtmpl': os.path.join(temp_dir, f'{unique_id}_%(title)s.%(ext)s')
                })
            else:
                # ✅ PRIORITIZE HIGHEST QUALITY VIDEO WITH AUDIO
                download_opts.update({
                    'format': (
                        'best[height>=1080][acodec!=none]/best[height>=720][acodec!=none]/'
                        'best[height>=480][acodec!=none]/best[acodec!=none]/'
                        'bestvideo[height>=1080]+bestaudio/bestvideo[height>=720]+bestaudio/'
                        'bestvideo+bestaudio/best'
                    ),
                    'outtmpl': os.path.join(temp_dir, f'{unique_id}_%(title)s.%(ext)s'),
                    'merge_output_format': 'mp4',  # ✅ ENSURE MP4 OUTPUT
                })
                    
            def progress_hook(d):
                try:
                    if d['status'] == 'downloading':
                        progress = 50
                        if '_percent_str' in d:
                            percent_str = d['_percent_str'].replace('%', '').strip()
                            try:
                                progress = float(percent_str)
                            except:
                                progress = 50
                                            
                        with job_lock:
                            if job_id in download_jobs:
                                download_jobs[job_id]['progress'] = min(progress, 95)
                                    
                    elif d['status'] == 'finished':
                        with job_lock:
                            if job_id in download_jobs:
                                download_jobs[job_id]['progress'] = 95
                                download_jobs[job_id]['status'] = 'processing'
                except Exception:
                    pass
                    
            download_opts['progress_hooks'] = [progress_hook]
                    
            with yt_dlp.YoutubeDL(download_opts) as ydl:
                info = ydl.extract_info(url, download=True)
            
                # Find the downloaded file
                downloaded_files = glob.glob(os.path.join(temp_dir, f'{unique_id}_*'))
                if not downloaded_files:
                    raise Exception("No files were downloaded")
            
                file_path = downloaded_files[0]
            
                if not os.path.exists(file_path):
                    raise Exception("Downloaded file not found")
                            
                file_size = os.path.getsize(file_path)
                title = info.get('title', 'Instagram Content')
                ext = 'mp3' if format_id == 'audio_only' else info.get('ext', 'mp4')
            
                # ✅ REGISTER FOR AUTO-CLEANUP
                cleanup_time = datetime.now() + timedelta(minutes=5)  # Auto-delete after 5 minutes
                with temp_files_lock:
                    temp_files[job_id] = {
                        'file_path': file_path,
                        'temp_dir': temp_dir,
                        'cleanup_time': cleanup_time,
                        'filename': os.path.basename(file_path)
                    }
                            
                with job_lock:
                    if job_id in download_jobs:
                        download_jobs[job_id].update({
                            'status': 'completed',
                            'progress': 100,
                            'file_path': file_path,
                            'filename': os.path.basename(file_path),
                            'file_size': file_size,
                            'temp_dir': temp_dir,
                            'info': {
                                'title': title,
                                'duration': info.get('duration'),
                                'uploader': info.get('uploader'),
                                'quality': info.get('height', 'Unknown'),
                                'format': info.get('format', 'Unknown')
                            }
                        })
                
                return file_path
                            
        except Exception as e:
            # ✅ CLEANUP ON ERROR
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except:
                    pass
                        
            with job_lock:
                if job_id in download_jobs:
                    download_jobs[job_id].update({
                        'status': 'failed',
                        'error': f'Download failed: {str(e)}'
                    })
            return None

# ✅ AUTO-CLEANUP BACKGROUND TASK
async def cleanup_temp_files():
    """Background task to clean up temporary files"""
    while True:
        try:
            current_time = datetime.now()
            files_to_cleanup = []
            
            with temp_files_lock:
                for job_id, file_info in temp_files.items():
                    if current_time > file_info['cleanup_time']:
                        files_to_cleanup.append(job_id)
            
            for job_id in files_to_cleanup:
                with temp_files_lock:
                    if job_id in temp_files:
                        file_info = temp_files[job_id]
                        try:
                            if os.path.exists(file_info['temp_dir']):
                                shutil.rmtree(file_info['temp_dir'])
                        except Exception as e:
                            print(f"Cleanup error for {job_id}: {e}")
                        finally:
                            del temp_files[job_id]
            
            await asyncio.sleep(60)  # Check every minute
        except Exception as e:
            print(f"Cleanup task error: {e}")
            await asyncio.sleep(60)

# Async wrappers
async def extract_info_async(url, content_type=None):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, downloader.extract_info, url, content_type)

async def download_video_async(url, format_id, job_id, content_type=None):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, downloader.download_video_to_temp, url, format_id, job_id, content_type)

downloader = InstagramDownloader()

@app.on_event("startup")
async def startup_event():
    """Start background cleanup task"""
    asyncio.create_task(cleanup_temp_files())

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/extract")
async def extract_video_info(request_data: URLRequest):
    """Extract video information - High Quality Focus"""
    try:
        url = request_data.url.strip()
        content_type = request_data.content_type
        original_url = request_data.original_url or url
        
        if not url:
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': 'Please provide a URL',
                    'error_type': 'missing_url'
                }
            )
        
        if not downloader.is_valid_instagram_url(url):
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': 'Please provide a valid Instagram URL',
                    'error_type': 'invalid_url'
                }
            )
        
        if not content_type:
            content_type = downloader.detect_content_type(url)
        
        # ✅ STORIES HANDLING
        if content_type == 'story':
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': 'Instagram Stories Not Supported',
                    'error_type': 'story_not_supported',
                    'message': 'Instagram Stories cannot be downloaded due to platform restrictions.',
                    'details': {
                        'reasons': [
                            'Stories require Instagram login and authentication',
                            'Stories expire after 24 hours',
                            'Most stories are private or restricted',
                            'Instagram has strict anti-scraping measures'
                        ],
                        'alternatives': [
                            'Try downloading Instagram Reels instead',
                            'Use Instagram Posts (they work reliably)',
                            'Check if the content is available as a Reel'
                        ]
                    },
                    'recommendation': 'Please try using Instagram Reels or Posts instead!'
                }
            )
        
        # Extract info for other content
        result = await extract_info_async(url, content_type)
        
        if not result['success']:
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': result['error'],
                    'error_type': result.get('error_type', 'unknown'),
                    'suggestion': 'Try using a different public Instagram post or reel.'
                }
            )
        
        info = result['data']
        
        # ✅ ENHANCED FORMAT PROCESSING FOR HIGHEST QUALITY
        formats = []

        if 'formats' in info and info['formats']:
            # Filter and sort for highest quality video formats
            video_formats = []
            
            for fmt in info['formats']:
                if (fmt.get('vcodec') != 'none' and 
                    fmt.get('acodec') != 'none' and 
                    fmt.get('height') and fmt.get('width')):
                    
                    # ✅ SAFE QUALITY SCORE CALCULATION WITH NONE CHECKS
                    height = fmt.get('height') or 0
                    width = fmt.get('width') or 0
                    tbr = fmt.get('tbr') or 0
                    
                    # Ensure all values are numbers
                    try:
                        height = int(height) if height else 0
                        width = int(width) if width else 0
                        tbr = float(tbr) if tbr else 0
                    except (ValueError, TypeError):
                        height = width = tbr = 0
                    
                    quality_score = (height * width) + (tbr * 1000)
                    fmt['quality_score'] = quality_score
                    video_formats.append(fmt)
            
            # Sort by quality score (highest first)
            video_formats.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
            
            # Select top quality formats
            seen_heights = set()
            for fmt in video_formats:
                height = fmt.get('height') or 0
                try:
                    height = int(height) if height else 0
                except (ValueError, TypeError):
                    height = 0
                    
                if height and height not in seen_heights and len(formats) < 4:
                    seen_heights.add(height)
                    
                    # ✅ ENHANCED QUALITY LABELS
                    if height >= 1080:
                        quality_label = f"Ultra HD ({height}p) - Best Quality"
                    elif height >= 720:
                        quality_label = f"Full HD ({height}p) - High Quality"
                    elif height >= 480:
                        quality_label = f"HD ({height}p) - Good Quality"
                    else:
                        quality_label = f"Standard ({height}p)"
                    
                    formats.append({
                        'format_id': fmt.get('format_id'),
                        'ext': fmt.get('ext', 'mp4'),
                        'quality': quality_label,
                        'filesize': fmt.get('filesize'),
                        'width': fmt.get('width'),
                        'height': fmt.get('height'),
                        'tbr': fmt.get('tbr'),
                        'type': 'video'
                    })
        
        # ✅ ALWAYS ADD BEST QUALITY OPTION
        if not formats:
            formats.append({
                'format_id': 'best',
                'ext': 'mp4',
                'quality': 'Highest Available Quality (Auto)',
                'type': 'video'
            })
        
        # Add audio option
        formats.append({
            'format_id': 'audio_only',
            'ext': 'mp3',
            'quality': 'Audio Only (320kbps MP3)',
            'type': 'audio'
        })
        
        # Create session
        session_id = str(uuid.uuid4())
        video_cache[session_id] = {
            'url': url,
            'content_type': content_type,
            'info': {
                'title': info.get('title', 'Instagram Content'),
                'duration': info.get('duration'),
                'thumbnail': info.get('thumbnail'),
                'uploader': info.get('uploader'),
                'view_count': info.get('view_count'),
                'formats': formats,
                'content_type': content_type
            }
        }
        
        return JSONResponse(
            status_code=200,
            content={
                'success': True,
                'session_id': session_id,
                'video_info': video_cache[session_id]['info'],
                'content_type': content_type,
                'quality_note': 'All downloads are optimized for highest available quality'
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=200,
            content={
                'success': False,
                'error': f'Server error: {str(e)}',
                'error_type': 'server_error'
            }
        )

@app.post("/download")
async def download_video_endpoint(request_data: DownloadRequest, background_tasks: BackgroundTasks):
    """Start download process"""
    try:
        session_id = request_data.session_id
        format_id = request_data.format_id
        
        if not session_id or session_id not in video_cache:
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': 'Invalid session',
                    'error_type': 'invalid_session'
                }
            )
        
        cached_data = video_cache[session_id]
        content_type = cached_data.get('content_type', 'unknown')
        
        if content_type == 'story':
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': 'Story downloads are not supported',
                    'error_type': 'story_download_blocked'
                }
            )
        
        job_id = str(uuid.uuid4())
        
        with job_lock:
            download_jobs[job_id] = {
                'status': 'queued',
                'progress': 0,
                'video_title': cached_data['info'].get('title', 'Instagram Content'),
                'format_id': format_id,
                'content_type': content_type,
                'created_at': datetime.now().isoformat()
            }
        
        background_tasks.add_task(
            download_video_async, 
            cached_data['url'], 
            format_id, 
            job_id, 
            content_type
        )
        
        return JSONResponse(
            status_code=200,
            content={
                'success': True,
                'job_id': job_id,
                'message': f'High-quality download started for Instagram {content_type}'
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=200,
            content={
                'success': False,
                'error': f'Download start failed: {str(e)}',
                'error_type': 'download_start_error'
            }
        )

@app.get("/status/{job_id}")
async def get_download_status(job_id: str):
    """Check download status"""
    with job_lock:
        if job_id not in download_jobs:
            return JSONResponse(
                status_code=200,
                content={
                    'success': False,
                    'error': 'Job not found',
                    'error_type': 'job_not_found'
                }
            )
        
        job_data = download_jobs[job_id].copy()
        
        # Add download URL if completed
        if job_data['status'] == 'completed':
            job_data['download_url'] = f"/download-file/{job_id}"
        
        return JSONResponse(
            status_code=200,
            content={
                'success': True,
                'job_id': job_id,
                **job_data
            }
        )

@app.get("/download-file/{job_id}")
async def download_file(job_id: str):
    """✅ STREAM FILE AND AUTO-DELETE"""
    try:
        with temp_files_lock:
            if job_id not in temp_files:
                raise HTTPException(status_code=404, detail="File not found or expired")
            
            file_info = temp_files[job_id]
            file_path = file_info['file_path']
            filename = file_info['filename']
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")
        
        # ✅ STREAM FILE CONTENT
        def file_generator():
            try:
                with open(file_path, 'rb') as file:
                    while chunk := file.read(8192):  # 8KB chunks
                        yield chunk
            finally:
                # ✅ IMMEDIATE CLEANUP AFTER STREAMING
                try:
                    if os.path.exists(file_info['temp_dir']):
                        shutil.rmtree(file_info['temp_dir'])
                    with temp_files_lock:
                        if job_id in temp_files:
                            del temp_files[job_id]
                except Exception as cleanup_error:
                    print(f"Cleanup error: {cleanup_error}")
        
        # Determine content type
        content_type = "video/mp4"
        if filename.endswith('.mp3'):
            content_type = "audio/mpeg"
        elif filename.endswith('.webm'):
            content_type = "video/webm"
        
        return StreamingResponse(
            file_generator(),
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET",
                "Access-Control-Allow-Headers": "*",
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error serving file: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check with cleanup info"""
    with temp_files_lock:
        temp_files_count = len(temp_files)
    
    return JSONResponse(
        status_code=200,
        content={
            'status': 'OK',
            'message': 'Instagram Downloader API - High Quality & Auto Cleanup',
            'features': [
                'Highest quality video downloads',
                'Automatic file cleanup (5 min)',
                'No permanent storage',
                'Streaming downloads'
            ],
            'supported_content': ['posts', 'reels', 'igtv'],
            'not_supported': ['stories'],
            'active_downloads': len(download_jobs),
            'temp_files': temp_files_count,
            'quality_priority': 'Ultra HD (1080p+) > Full HD (720p) > HD (480p)'
        }
    )

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)