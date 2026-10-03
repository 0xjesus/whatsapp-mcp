"""Synthetic end-to-end retrieval regressions; no private messages or real inference."""
import os
import unittest
import uuid


class PlanValidationTests(unittest.TestCase):
    def test_unknown_plan_fields_are_rejected(self):
        from server import search
        from unittest.mock import Mock
        store=Mock();store.lexical.return_value=[];store.semantic.return_value=[]
        model=Mock();model.embed.return_value=[[1.]+[0.]*1023]
        with self.assertRaises(ValueError):
            search(store,model,{'source':'whatsapp','query':'test','plan':{'sql':'SELECT 1'}})

    def test_fallback_preserves_spanish_accents_for_fts(self):
        from query_plan import prepare
        self.assertIn('cumpleaños',prepare('instrucciones para mi cumpleaños')['keywords'])

    def test_generic_greetings_are_not_event_answers(self):
        from query_plan import informative
        for text in ['Hola amor, buen día','Buenos días, que tengas un bonito día','Feliz día mi amor']:
            self.assertFalse(informative({'text':text},'instrucciones para mi cumpleaños'),text)
        self.assertTrue(informative({'text':'Feliz cumpleaños, que disfrutes tu fiesta'},'instrucciones para mi cumpleaños'))

    def test_reversed_dates_are_rejected(self):
        from server import request_filters
        with self.assertRaises(ValueError):
            request_filters({'source':'telegram','after':'2026-05-09','before':'2026-05-01'})

    def test_evidence_budget_preserves_references_and_utf8(self):
        from search_engine import bound_evidence
        rows=[{'message_id':str(i),'text':'música 🎵 '*1000,'matched_fragment':'dirección '*500} for i in range(8)]
        rows.append({'message_id':'short','text':'Llega después de las siete.'})
        metadata=bound_evidence(rows,budget=2000)
        self.assertLessEqual(metadata['evidence_text_bytes'],2000)
        self.assertTrue(metadata['evidence_truncated'])
        self.assertEqual(rows[-1]['text'],'Llega después de las siete.')
        self.assertEqual([r['message_id'] for r in rows],[str(i) for i in range(8)]+['short'])
        self.assertTrue(all(r['text'] for r in rows))
        self.assertTrue(all(r['matched_fragment'] and r['text_truncated'] for r in rows[:-1]))


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'),'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class SearchEngineTests(unittest.TestCase):
    def setUp(self):
        from store import Store,digest
        self.store=Store(os.environ['MEMORY_TEST_DSN'],'memory_test_retrieval_'+uuid.uuid4().hex)
        self.store.initialize()
        self.base=1777975200
        self.rows=[self.row('anchor','Feliz cumpleaños, espero que disfrutes la celebración',0),
                   self.row('instruction','No lleves equipo de sonido, ya tenemos todo',7*3600),
                   self.row('arrival','Puedes llegar después de las siete',11*3600),
                   self.row('confirmation','Hay sonido y micrófonos disponibles',7*3600+20),
                   self.row('greeting','Bien amor gracias',1),
                   self.row('outside','Lleva el equipo el próximo mes',20*86400),
                   self.row('other','No lleves equipo de sonido',7*3600,chat='other',name='Bea')]
        self.store.apply(self.rows,'whatsapp',backfill_done=True)
        self.vector=[1.]+[0.]*1023
        self.store.save_embeddings([(digest('Bien amor gracias'),self.vector)])
        class Model:
            def embed(inner,texts,interactive=False,**kwargs):return [self.vector for _ in texts]
        self.model=Model()

    def tearDown(self):
        self.store.drop_test_schema()

    def row(self,key,text,offset,chat='ana',name='Ana'):
        return dict(source='whatsapp',chat_id=chat,message_id=key,timestamp=self.base+offset,
                    sender='person-'+chat,chat_name=name,text=text,media_type='')

    def request(self,**changes):
        body={'source':'whatsapp','query':'busca las instrucciones que me dijo Ana para mi cumpleaños',
              'plan':{'person':'Ana','keywords':['cumpleaños','celebración'],
                      'semantic_queries':['instrucciones para una celebración'],
                      'context':'event','context_terms':['llevar','llegar']},'limit':4}
        body.update(changes)
        from server import search
        return search(self.store,self.model,body)

    def test_natural_question_finds_unembedded_instructions_through_event_context(self):
        result=self.request()
        self.assertEqual(result.get('plan_source'),'requesting_ai')
        rows=result['results']+[c for row in result['results'] for c in row.get('context',[])]
        keys={r['message_id'] for r in rows}
        self.assertTrue({'anchor','instruction','arrival','confirmation'}<=keys,keys)
        self.assertNotIn('other',keys)
        self.assertNotIn('outside',keys)
        self.assertNotIn('greeting',{r['message_id'] for r in result['results']})
        self.assertTrue(all(r['source']=='whatsapp' and r['chat_id']=='ana' for r in rows))
        self.assertFalse(next(r for r in rows if r['message_id']=='instruction')['embedding_ready'])
        self.assertIn('timings_ms',result)

    def test_hybrid_recovers_text_and_context_when_model_is_down(self):
        def down(*a,**kw):raise TimeoutError('synthetic')
        self.model.embed=down
        result=self.request()
        self.assertTrue(result['degraded'])
        self.assertIn('anchor',{r['message_id'] for r in result['results']})
        self.assertEqual(result.get('semantic_status'),'unavailable')

    def test_ambiguous_person_returns_candidates_without_searching_unrelated_chats(self):
        self.store.apply([self.row('second','hello',0,chat='ana2',name='Ana Pérez')],'whatsapp')
        result=self.request()
        self.assertEqual(result.get('status'),'needs_disambiguation')
        self.assertEqual(len(result['chat_candidates']),2)
        self.assertEqual(result['results'],[])

    def test_explicit_chat_is_authoritative_and_dates_limit_context(self):
        from datetime import datetime,timezone
        bound=datetime.fromtimestamp(self.base+8*3600,timezone.utc).isoformat()
        result=self.request(chat='ana',before=bound)
        rows=result['results']+[c for row in result['results'] for c in row.get('context',[])]
        self.assertIn('instruction',{r['message_id'] for r in rows})
        self.assertNotIn('arrival',{r['message_id'] for r in rows})
        self.assertTrue(all(r['timestamp']<=self.base+8*3600 for r in rows))

    def test_literal_plan_terms_cannot_become_sql_or_boolean_operators(self):
        result=self.request(plan={'keywords':['x\" OR celebration --'],'context':'none'},mode='keyword',chat='ana')
        self.assertEqual(result['results'],[])

    def test_natural_spanish_without_a_plan_survives_model_failure(self):
        def down(*a,**kw):raise TimeoutError('synthetic')
        self.model.embed=down
        result=self.request(plan=None)
        self.assertEqual(result['plan_source'],'deterministic_fallback')
        self.assertIn('anchor',{r['message_id'] for r in result['results']})

    def test_context_terms_match_accents_consistently(self):
        self.store.apply([self.row('accent','Habrá música de piano',3600)],'whatsapp')
        result=self.request(plan={'person':'Ana','keywords':['cumpleaños'],'context':'event','context_terms':['música']},mode='keyword')
        contexts=[r for item in result['results'] for r in item.get('context',[])]
        self.assertIn('accent',{r['message_id'] for r in contexts})

    def test_semantic_only_abstains_from_generic_greetings(self):
        result=self.request(mode='semantic',chat='ana')
        self.assertEqual(result['results'],[])
        self.assertEqual(result.get('status'),'no_relevant_matches')

    def test_near_duplicate_rewrites_do_not_double_the_semantic_vote(self):
        from unittest.mock import patch
        with self.store.connection() as db:
            rows=db.execute('SELECT * FROM messages WHERE chat_id=%s ORDER BY timestamp',('ana',)).fetchall()
        anchor=next(r for r in rows if r['message_id']=='anchor')
        candidate=next(r for r in rows if r['message_id']=='instruction')
        candidate.update(score=.65)
        with patch.object(self.store,'lexical',return_value=[anchor]),patch.object(self.store,'semantic',return_value=[candidate]):
            result=self.request(chat='ana',plan={'keywords':['cumpleaños'],
                'semantic_queries':['instrucciones para la fiesta','indicaciones para la celebración'],
                'context':'none'})
        self.assertEqual(result['results'][0]['message_id'],'anchor')
        self.assertAlmostEqual(result['results'][1]['rank_score'],1/61)

    def test_context_budget_reserves_space_for_each_ranked_event(self):
        from unittest.mock import patch
        from search_data import SearchData
        anchors=[dict(self.row('event-'+str(i),'Feliz cumpleaños',i*10*86400),id=100+i) for i in range(4)]
        def context(data,row,filters,intent,terms,limit=12,max_messages=24):
            return [dict(row,id=row['id']*100+j,message_id='context-'+str(row['id'])+'-'+str(j)) for j in range(24)]
        with patch.object(self.store,'lexical',return_value=anchors),patch.object(SearchData,'context',context):
            result=self.request(mode='keyword',chat='ana')
        self.assertTrue(all(row.get('context') for row in result['results']))
        self.assertLessEqual(sum(len(row['context']) for row in result['results']),48)


if __name__=='__main__':unittest.main()
