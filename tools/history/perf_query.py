"""Build the filtered semantic query independently of connection management."""


FILTERED_CHUNK_LIMIT = 50_000


def semantic_statement(where, values, literal, limit):
    """Accept only Store.filters-generated SQL; bind all user values separately.

    Selective filters first collect a bounded set of message/chunk references.
    If it fits, score its unique hashes exactly and rank the matching messages.
    This avoids HNSW filter starvation and looking up shared fragments in every
    other chat. An overflow uses the existing approximate search, without
    silently discarding matching messages beyond the bounded probe.

    For approximate search, each hash needs at most limit matching messages.
    A later message for that same hash already has limit distinct messages ahead
    of it at an equal or better distance. Deduplicate those bounded candidates,
    then apply the final result limit before fetching message/fragment text.
    """
    limit = max(1, min(int(limit), 100))
    values = list(values)
    prefix, guard, exact_candidates, params = '', '', '', []
    if any(column in where for column in ('m.chat_id', 'm.timestamp', 'm.sender')):
        prefix = '''filtered_chunks AS MATERIALIZED (
            SELECT m.id,m.timestamp,c.hash
            FROM messages m JOIN message_chunks c ON c.message_pk=m.id
            WHERE ''' + where + ''' LIMIT %s),
        strategy AS MATERIALIZED (
            SELECT count(*)<=%s AS use_exact FROM filtered_chunks),
        filtered_hashes AS MATERIALIZED (
            SELECT DISTINCT hash FROM filtered_chunks
            WHERE (SELECT use_exact FROM strategy)),
        scored_hashes AS MATERIALIZED (
            SELECT e.hash,e.embedding <=> %s::halfvec AS distance
            FROM filtered_hashes f JOIN embeddings e ON e.hash=f.hash
            WHERE e.embedding IS NOT NULL),
        '''
        params = values + [FILTERED_CHUNK_LIMIT + 1, FILTERED_CHUNK_LIMIT, literal]
        guard = ' AND NOT (SELECT use_exact FROM strategy)'
        exact_candidates = '''SELECT f.id,f.timestamp,h.hash,h.distance
            FROM filtered_chunks f JOIN scored_hashes h ON h.hash=f.hash
            UNION ALL
            '''
    # Bound ANN overfetch to the requested result budget. Requiring 100 matching
    # hashes even for six results made source-filtered HNSW walks exceed 45 s
    # on a growing, disk-backed index; 20 retained the same top results in <1 s.
    sql = 'WITH ' + prefix + '''nearest AS MATERIALIZED (
        SELECT e.hash,e.embedding <=> %s::halfvec AS distance FROM embeddings e
        WHERE e.embedding IS NOT NULL''' + guard + ''' AND EXISTS(
            SELECT 1 FROM message_chunks c JOIN messages m ON m.id=c.message_pk
            WHERE c.hash=e.hash AND ''' + where + ''')
        ORDER BY e.embedding <=> %s::halfvec LIMIT %s),
        candidates AS MATERIALIZED (
            ''' + exact_candidates + '''SELECT m.id,m.timestamp,n.hash,n.distance FROM nearest n
            CROSS JOIN LATERAL (
                SELECT DISTINCT m.id,m.timestamp
                FROM message_chunks c JOIN messages m ON m.id=c.message_pk
                WHERE c.hash=n.hash AND ''' + where + '''
                ORDER BY m.timestamp DESC,m.id DESC LIMIT %s
            ) m),
        best AS MATERIALIZED (
            SELECT DISTINCT ON(id) id,timestamp,hash,distance FROM candidates
            ORDER BY id,distance,hash),
        ranked AS MATERIALIZED (
            SELECT * FROM best ORDER BY distance,timestamp DESC,id DESC LIMIT %s)
        SELECT m.id,m.source,m.chat_id,m.message_id,m.timestamp,m.sender,m.chat_name,
            left(m.text,4000) AS text,m.media_type,e.text AS matched_fragment,1-r.distance AS score
        FROM ranked r JOIN messages m ON m.id=r.id JOIN embeddings e ON e.hash=r.hash
        ORDER BY r.distance,r.timestamp DESC,r.id DESC'''
    return sql, params + [literal] + values + [literal,min(60,max(20,limit*2))] + values + [limit,limit]
