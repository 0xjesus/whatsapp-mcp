#!/usr/bin/env python3
"""Transcriptor opcional de notas de voz y audio de videos para whatsapp-mcp.

Vigila messages.db, descarga cada audio o video vía el propio MCP (HTTP local),
lo transcribe con faster-whisper `small` int8 en CPU (2 hilos, prioridad
mínima) y escribe el texto en messages.content con un marcador, de modo
que list_messages / búsqueda / contexto del MCP lo devuelven sin cambios en Go.
La tabla `transcripts` es la fuente de verdad y permite re-aplicar el texto si
una resincronización deja el content vacío. Los videos conservan su caption;
ffmpeg extrae el audio y se rechazan clips de más de 600 segundos.
"""
import json, os, sqlite3, sys, time, urllib.request, logging, datetime as dt
import hashlib
from pathlib import Path
import subprocess

STORE = os.path.expanduser(os.environ.get("WA_STORE", "~/.local/share/whatsapp-mcp/store"))
DB = os.path.join(STORE, "messages.db")
MCP = os.environ.get("WA_MCP_URL", "http://127.0.0.1:8765/mcp")
MEDIA_ROOT = os.path.join(STORE, "uploads")
TMP = os.path.join(MEDIA_ROOT, "_transcribe")
MODEL = os.environ.get("WA_WHISPER_MODEL", "small")
THREADS = int(os.environ.get("WA_WHISPER_THREADS", "2"))
WINDOW_DAYS = int(os.environ.get("WA_WINDOW_DAYS", "60"))
POLL = int(os.environ.get("WA_POLL_SECONDS", "30"))
BATCH = int(os.environ.get("WA_BATCH", "4"))
PREFIX = "[Nota de voz] "
VIDEO_PREFIX = "[Audio del video] "
FFMPEG = os.environ.get('WA_FFMPEG', 'ffmpeg')
MAX_VIDEO_SECONDS = 600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("transcriber")


class Mcp:
    def __init__(self):
        self.session = None

    def _init(self):
        req = urllib.request.Request(MCP, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "transcriber", "version": "1"}}}).encode(),
                                     headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream"})
        with urllib.request.urlopen(req, timeout=30) as r:
            self.session = r.headers.get("Mcp-Session-Id")
        log.info("sesión MCP nueva")

    def call(self, name, args):
        body = json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": name, "arguments": args}}).encode()
        for attempt in range(2):
            if not self.session:
                self._init()
            req = urllib.request.Request(MCP, data=body, headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "Mcp-Session-Id": self.session})
            try:
                with urllib.request.urlopen(req, timeout=180) as r:
                    return json.load(r)
            except urllib.error.HTTPError as error:
                if error.code in (400, 404):
                    self.session = None
                    if attempt == 0:
                        continue
                raise


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS transcripts(
        message_id TEXT NOT NULL, chat_jid TEXT NOT NULL, text TEXT, model TEXT,
        status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
        created_at TEXT, updated_at TEXT, PRIMARY KEY(message_id, chat_jid))""")
    return c


def pending(c):
    since = (dt.datetime.now() - dt.timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
    return c.execute("""
        SELECT m.id, m.chat_jid, m.timestamp, m.content, t.status, t.attempts, t.text, m.media_type
        FROM messages m LEFT JOIN transcripts t ON t.message_id = m.id AND t.chat_jid = m.chat_jid
        WHERE m.media_type IN ('audio','video') AND m.timestamp >= ? AND m.chat_jid != 'status@broadcast'
          AND (t.status IS NULL OR t.status = 'failed'
               OR (t.status = 'done' AND COALESCE(trim(t.text), '') != ''
                   AND (m.content IS NULL OR m.content = '' OR
                       (m.media_type='video' AND m.content NOT LIKE '[Audio del video] %'
                        AND instr(m.content,char(10)||'[Audio del video] ')=0))))
        ORDER BY m.timestamp DESC LIMIT ?""", (since, BATCH)).fetchall()


def apply_text(c, mid, jid, text, media_type='audio'):
    if media_type == 'video':
        spoken = VIDEO_PREFIX + text
        # Read/append in one UPDATE so concurrent caption edits are preserved.
        c.execute("""UPDATE messages SET content=CASE WHEN COALESCE(content,'')='' THEN ?
                     ELSE content||char(10)||? END
                     WHERE id=? AND chat_jid=? AND COALESCE(content,'') NOT LIKE ?
                     AND instr(COALESCE(content,''),char(10)||?)=0""",
                  (spoken,spoken,mid,jid,VIDEO_PREFIX+'%',VIDEO_PREFIX))
    else:
        c.execute("UPDATE messages SET content = ? WHERE id = ? AND chat_jid = ? AND (content IS NULL OR content = '' OR content LIKE ?)",
                  (PREFIX + text, mid, jid, PREFIX + "%"))


def extract_audio(src, dst):
    """Extract video audio atomically; the extra second detects oversized clips."""
    dst = Path(dst)
    part = dst.with_suffix(dst.suffix + '.part')
    try:
        result = subprocess.run([FFMPEG,'-nostdin','-v','error','-y','-i',str(src),'-vn',
                                 '-ac','1','-ar','16000','-t',str(MAX_VIDEO_SECONDS+1),
                                 '-c:a','libopus','-b:a','24k','-f','ogg',str(part)],
                                capture_output=True, timeout=120)
        if result.returncode or not part.exists() or not part.stat().st_size:
            no_audio = b'does not contain any stream' in (result.stderr or b'')
            raise ValueError('no_audio' if no_audio else 'invalid_audio')
        part.replace(dst)
    finally:
        part.unlink(missing_ok=True)


def run_once(c, model, mcp):
    """Process a bounded batch, including completed-text repairs without inference."""
    for mid, jid, ts, content, status, attempts, prev_text, media_type in pending(c):
        now = dt.datetime.now().isoformat(timespec='seconds')
        if status == 'done':
            apply_text(c, mid, jid, prev_text, media_type)
            c.commit()
            continue
        if (attempts or 0) >= 3:
            c.execute("UPDATE transcripts SET status='failed_permanent', updated_at=? WHERE message_id=? AND chat_jid=?", (now,mid,jid))
            c.commit()
            continue
        # Message ids alone may collide across chats and are not safe filenames.
        key = hashlib.sha256((jid+'\0'+mid).encode()).hexdigest()
        path = Path(TMP)/(key+'.ogg')
        video = path.with_suffix('.mp4')
        try:
            if not path.exists():
                download = video if media_type == 'video' else path
                mcp.call('download_media', {'chat_jid':jid,'message_id':mid,'output_path':str(download)})
                if not download.is_file():
                    raise RuntimeError('download_unavailable')
                if media_type == 'video':
                    extract_audio(video,path)
            segments, info = model.transcribe(str(path), language=None, vad_filter=True, beam_size=1)
            if media_type == 'video' and info.duration > MAX_VIDEO_SECONDS:
                raise ValueError('video_too_long')
            text = ' '.join(s.text.strip() for s in segments).strip()
            c.execute("""INSERT INTO transcripts(message_id,chat_jid,text,model,status,attempts,error,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(message_id,chat_jid) DO UPDATE SET text=excluded.text,
                model=excluded.model,status='done',attempts=excluded.attempts,error=NULL,updated_at=excluded.updated_at""",
                (mid,jid,text,MODEL,'done',(attempts or 0)+1,None,now,now))
            if text:
                apply_text(c,mid,jid,text,media_type)
            c.commit()
            path.unlink(missing_ok=True)
            log.info('transcripción completada (%s)', media_type)
        except Exception as error:
            reason = str(error) if isinstance(error,ValueError) and str(error) in ('no_audio','video_too_long','invalid_audio') else type(error).__name__
            terminal = reason in ('no_audio','video_too_long') or (attempts or 0)+1 >= 3
            c.execute("""INSERT INTO transcripts(message_id,chat_jid,status,attempts,error,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(message_id,chat_jid) DO UPDATE SET status=excluded.status,
                attempts=excluded.attempts,error=excluded.error,updated_at=excluded.updated_at""",
                (mid,jid,'failed_permanent' if terminal else 'failed',(attempts or 0)+1,reason,now,now))
            c.commit()
            if terminal:
                path.unlink(missing_ok=True)
            log.warning('transcripción pendiente: %s', reason)
        finally:
            video.unlink(missing_ok=True)


def main():
    from faster_whisper import WhisperModel
    os.umask(0o077)
    os.makedirs(TMP, mode=0o700, exist_ok=True)
    os.chmod(TMP, 0o700)
    log.info("cargando modelo %s (cpu int8, %d hilos)", MODEL, THREADS)
    model = WhisperModel(MODEL, device="cpu", compute_type="int8", cpu_threads=THREADS, num_workers=1)
    mcp = Mcp()
    log.info("listo; ventana %d días, cada %ds", WINDOW_DAYS, POLL)
    while True:
        try:
            c = db()
            try:
                run_once(c, model, mcp)
            finally:
                c.close()
        except Exception as e:  # noqa: BLE001
            log.error("ciclo: %s", e)
        time.sleep(POLL)


if __name__ == "__main__":
    main()
