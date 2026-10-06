"""Read-only lookup/context operations separate from ranking and plan execution."""
import threading
import time
from query_plan import fold,words,context_prefixes

DIRECTORIES={}
DIRECTORY_LOCK=threading.Lock()
COLUMNS='m.id,m.source,m.chat_id,m.message_id,m.timestamp,m.sender,m.chat_name,left(m.text,4000) AS text,m.media_type'


class SearchData:
    def __init__(self,store,deadline=None):self.store,self.deadline=store,deadline

    def connection(self,maximum):
        remaining=maximum if self.deadline is None else min(maximum,self.deadline-time.monotonic())
        if remaining<=.05:raise TimeoutError('Query budget exhausted')
        return self.store.connection(str(max(1,int(remaining*1000)))+'ms')

    def directory(self,source):
        key=(self.store.schema,source)
        with DIRECTORY_LOCK:
            hit=DIRECTORIES.get(key)
            # Every directory includes a consent-controlled source; read current policy.
        where,values=self.store.filters(source=source)
        with self.connection(8) as db:
            rows=db.execute('SELECT DISTINCT ON(chat_id) chat_id,chat_name FROM messages m WHERE '+where+' ORDER BY chat_id DESC,timestamp DESC LIMIT 10000',values).fetchall()
        with DIRECTORY_LOCK:
            if len(DIRECTORIES)>=16:DIRECTORIES.pop(next(iter(DIRECTORIES)))
            DIRECTORIES[key]=(time.monotonic(),rows)
        return rows

    def resolve(self,source,hint,explicit=False):
        if explicit:
            with self.connection(4) as db:
                where,values=self.store.filters(source=source,chat=hint)
                row=db.execute('SELECT chat_id,chat_name FROM messages m WHERE '+where+' LIMIT 1',values).fetchone()
            if row:return [row]
        wanted=fold(hint).strip()
        return [r for r in self.directory(source) if wanted in fold(r['chat_name'])][:6]

    def infer_person(self,source,query):
        # Only explicit capitalized name tokens, never silently infer dates.
        tokens={fold(w) for w in query.split() if len(w)>3 and w[0].isupper()}
        if not tokens:return None
        matches=[r for r in self.directory(source) if words(r['chat_name']) and words(r['chat_name'])[0] in tokens]
        if not matches:return None
        first={words(r['chat_name'])[0] for r in matches}
        return next(iter(first)) if len(first)==1 else None

    def context(self,row,filters,intent,terms,limit=12,max_messages=24):
        selected=dict(filters,source=row['source'],chat=row['chat_id'])
        where,values=self.store.filters(**selected)
        with self.connection(4) as db:
            if intent=='event':
                prefixes=context_prefixes(terms)
                if not prefixes:prefixes=['llev','lleg','hora','lugar','direc','prepar','necesit','favor']
                sql='''WITH matches AS MATERIALIZED (
                    SELECT m.id,m.timestamp FROM messages m WHERE '''+where+'''
                    AND m.timestamp BETWEEN %s AND %s AND m.id<>%s
                    AND translate(lower(m.text),'áéíóúüñ','aeiouun') LIKE ANY(%s)
                    ORDER BY abs(m.timestamp-%s),m.timestamp,m.id LIMIT %s),
                expanded AS (
                    SELECT id,timestamp,0 AS priority FROM matches
                    UNION ALL
                    SELECT n.id,n.timestamp,1 FROM matches a CROSS JOIN LATERAL (
                        (SELECT m.id,m.timestamp FROM messages m WHERE '''+where+'''
                         AND m.timestamp BETWEEN %s AND %s
                         AND (m.timestamp,m.id)<(a.timestamp,a.id)
                         ORDER BY m.timestamp DESC,m.id DESC LIMIT 1)
                        UNION ALL
                        (SELECT m.id,m.timestamp FROM messages m WHERE '''+where+'''
                         AND m.timestamp BETWEEN %s AND %s
                         AND (m.timestamp,m.id)>(a.timestamp,a.id)
                         ORDER BY m.timestamp,m.id LIMIT 1)) n),
                chosen AS (
                    SELECT id,min(priority) AS priority,min(abs(timestamp-%s)) AS distance
                    FROM expanded GROUP BY id ORDER BY priority,distance,id LIMIT %s)
                SELECT '''+COLUMNS+''' FROM chosen c JOIN messages m ON m.id=c.id
                ORDER BY m.timestamp,m.id'''
                return db.execute(sql,values+[
                    row['timestamp']-72*3600,row['timestamp']+72*3600,row['id'],['%'+p+'%' for p in prefixes],
                    row['timestamp'],min(limit,8)]+values+[row['timestamp']-72*3600,row['timestamp']+72*3600]+values+[row['timestamp']-72*3600,row['timestamp']+72*3600]+[row['timestamp'],min(24,max_messages)]).fetchall()
            rows=[]
            for op,order in [('<','DESC'),('>','ASC')]:
                rows.extend(db.execute('SELECT '+COLUMNS+' FROM messages m WHERE '+where+
                    ' AND (m.timestamp,m.id) '+op+' (%s,%s) ORDER BY m.timestamp '+order+',m.id '+order+' LIMIT %s',
                    values+[row['timestamp'],row['id'],limit//2]).fetchall())
            return sorted(rows,key=lambda r:(r['timestamp'],r['id']))

    def authorized(self, rows, *, source=None, chats=False):
        """Recheck retained evidence against one current policy snapshot before returning it."""
        if not rows:
            return []
        where, values = self.store.filters(source=source)
        column = 'chat_id' if chats else 'id'
        identities = list({r[column] for r in rows})
        with self.connection(4) as db:
            current = db.execute('SELECT DISTINCT m.id,m.source,m.chat_id FROM messages m WHERE ' + where +
                                 f' AND m.{column}=ANY(%s)', values + [identities]).fetchall()
        if chats:
            allowed = {r['chat_id'] for r in current}
            return [r for r in rows if r['chat_id'] in allowed]
        allowed = {(r['id'], r['source'], r['chat_id']) for r in current}
        return [r for r in rows if (r['id'], r['source'], r['chat_id']) in allowed]

    def coverage(self,rows):
        ids=list({r['id'] for r in rows if 'chat_id' in r})
        if not ids:return {}
        with self.connection(4) as db:
            return {r['message_pk']:r['ready'] for r in db.execute('''
                SELECT c.message_pk,bool_and(e.embedding IS NOT NULL) AS ready
                FROM message_chunks c JOIN embeddings e ON e.hash=c.hash
                WHERE c.message_pk=ANY(%s) GROUP BY c.message_pk''',(ids,)).fetchall()}
