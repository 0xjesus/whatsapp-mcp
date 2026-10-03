"""Validate plans authored by the requesting AI; transparent non-LLM fallback."""
import re
import unicodedata

STOP=set('a al algo ante asi aun aunque bajo bien busca buscar busqueda busca buscame cual cuales cuando con como cuanto de del desde donde el ella ellas ellos en entre era es esa ese eso esta estas este estos estoy fue fui ha hacia hasta hay hizo instrucciones indicaciones instruccion indicacion la las le les lo los me mensaje mensajes mi mis mientras muy no nos nosotros o otro para pero por porque que quien se si sin sobre son su sus te ti tiene todo tu tus un una unos unas ve ver ya yo dijo dime dame necesito quiero recuerda favor porfavor hola amor gracias buenos buenas dias tardes noches ok okay vale perfecto si'.split())
SMALLTALK=set('buen dia buenisimo bonito bonita lindo linda hermoso hermosa feliz tengas tengan deseando deseo saludos saludo besos beso abrazos abrazo igualmente igualmente'.split())


def fold(text):
    return ''.join(c for c in unicodedata.normalize('NFKD',str(text).lower()) if not unicodedata.combining(c))


def words(text):
    return re.findall(r'[^\W_]+',fold(text),flags=re.UNICODE)


def concepts(text):
    return [w for w in words(text) if len(w)>2 and w not in STOP]


def literal_concepts(text):
    # PostgreSQL simple FTS preserves accents; folding belongs in comparisons.
    tokens=re.findall(r'[^\W_]+',str(text).lower(),flags=re.UNICODE)
    return [w for w in tokens if len(w)>2 and fold(w) not in STOP] or tokens


def strings(value,maximum,length,*,bytes_limit=False):
    if not isinstance(value,list) or len(value)>maximum:
        raise ValueError('Invalid plan list')
    output=[]
    for item in value:
        if not isinstance(item,str) or not item.strip() or any(ord(c)<32 for c in item):
            raise ValueError('Invalid plan term')
        item=item.strip()
        if (len(item.encode()) if bytes_limit else len(item))>length:
            raise ValueError('Plan term too long')
        if item not in output:output.append(item)
    return output


def prepare(query,provided=None):
    if provided is not None:
        if not isinstance(provided,dict) or set(provided)-{'keywords','semantic_queries','person','context','context_terms'}:
            raise ValueError('Invalid search plan fields')
        keywords=strings(provided.get('keywords',[]),8,80)
        variants=strings(provided.get('semantic_queries',[]),2,600,bytes_limit=True)
        context_terms=strings(provided.get('context_terms',[]),6,80)
        person=provided.get('person')
        if person is not None and (not isinstance(person,str) or not person.strip() or len(person)>100 or any(ord(c)<32 for c in person)):
            raise ValueError('Invalid person hint')
        context=provided.get('context','neighbors')
        if context not in ('none','neighbors','event'):raise ValueError('Invalid context intent')
        origin='requesting_ai'
    else:
        keywords=[];variants=[];context_terms=[];person=None
        context='event' if re.search(r'instrucci|indicaci|fiesta|cumple|evento|celebr',fold(query)) else 'neighbors'
        origin='deterministic_fallback'
    if not keywords:
        keywords=list(dict.fromkeys(literal_concepts(query)))[:8]
    if not variants:variants=[query]
    if sum(len(q.encode())+4 for q in variants)>1600:
        raise ValueError('Semantic plan exceeds model batch')
    return dict(keywords=keywords,semantic_queries=variants,person=person.strip() if person else None,
                context=context,context_terms=context_terms,origin=origin)


def literal_query(terms):
    # Quotes make alternatives literal phrases rather than query operators.
    return ' OR '.join('"'+term.replace('"',' ')+'"' for term in terms)


def context_prefixes(terms):
    output=[]
    for term in terms:
        for word in concepts(term):
            if len(word)>4 and word.endswith(('ar','er','ir')):word=word[:-2]
            if word not in output:output.append(word)
    return output[:12]


def informative(row,query):
    if fold(query).strip() and fold(query).strip() in fold(row.get('text','')):return True
    terms=set(concepts(row.get('text','')))
    if terms and terms<=SMALLTALK and not (terms & set(concepts(query))):return False
    return bool(terms) and (len(terms)>=2 or bool(terms & set(concepts(query))))
