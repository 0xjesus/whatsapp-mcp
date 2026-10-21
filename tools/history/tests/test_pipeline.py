import importlib.util
import unittest
from unittest.mock import Mock, patch


class PipelineTests(unittest.TestCase):
    def test_portable_worker_accepts_explicit_paths_and_whatsapp_only(self):
        import json
        import tempfile
        from pathlib import Path
        import worker
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / 'database.json'
            config.write_text(json.dumps({'writer_dsn':'synthetic-dsn'}))
            stop = Mock()
            stop.is_set.return_value = True
            with patch.object(worker, 'ROOT', root), patch.object(worker, 'STOP', stop), \
                    patch.object(worker, 'Store') as store, patch.object(worker, 'Embedder') as model, \
                    patch.object(worker, 'Source') as source, patch.object(worker.threading, 'Thread'), \
                    patch.object(worker.signal, 'signal'):
                worker.run(root=root, config_path=config, source_paths={'whatsapp': root/'messages.db'})
                store.assert_called_once_with('synthetic-dsn')
                source.assert_called_once_with('whatsapp', root/'messages.db')
                model.assert_called_once_with(root)
                store.return_value.initialize.assert_called_once()

    def test_implementation_exists(self):
        self.assertIsNotNone(importlib.util.find_spec('worker'), 'resumable worker missing')
        self.assertIsNotNone(importlib.util.find_spec('server'), 'query API missing')

    def test_embedding_response_order_and_dimensions(self):
        from embeddings import decode_vectors
        data = {'data':[{'index':1,'embedding':[2.0]*1024},{'index':0,'embedding':[1.0]*1024}]}
        self.assertEqual(decode_vectors(data, 2)[0][0], 1.0)
        with self.assertRaises(ValueError):
            decode_vectors({'data':[{'index':0,'embedding':[1.0]}]},1)
        with self.assertRaises(ValueError):
            decode_vectors({'data':[{'index':0,'embedding':[1.0]*1024}]*2},2)

    def test_batch_bounds_and_preserves_remainder(self):
        from embeddings import bounded_batch
        rows=[dict(hash=str(i),text='a'*600) for i in range(5)]
        selected=bounded_batch(rows)
        self.assertEqual([r['hash'] for r in selected],['0','1'])
        self.assertLessEqual(sum(len(r['text'].encode())+4 for r in selected),1600)

    def test_date_filter_end_of_day_and_invalid_source(self):
        from server import request_filters
        a=request_filters({'source':'telegram','after':'2026-01-01','before':'2026-01-01'})
        self.assertEqual(a['before']-a['after'],86399)
        with self.assertRaises(ValueError):
            request_filters({'source':'unknown'})

    def test_history_replay_uses_current_state_and_advances_only_after_apply(self):
        from worker import ingest_step
        source=Mock(kind='telegram')
        source.changes.return_value=[dict(seq=11,chat_id='a',message_id='1',op='delete',event_token='new')]
        source.get_many.return_value=[{'source':'telegram','chat_id':'a','message_id':'1','text':'recreated'}]
        store=Mock()
        store.state.return_value={'backfill_done':True,'cursor':10,'change_seq':10}
        self.assertTrue(ingest_step(store,source))
        self.assertEqual(store.apply.call_args.kwargs['change_seq'],11)
        self.assertEqual(store.apply.call_args.kwargs['change_token'],'new')
        self.assertEqual(store.apply.call_args.args[0][0]['text'],'recreated')
        self.assertEqual(store.apply.call_args.kwargs['deleted'],[])

    def test_ingestion_reads_one_deduplicated_page_in_event_order(self):
        from worker import ingest_step
        source=Mock(kind='telegram')
        source.changes.return_value=[dict(seq=seq,chat_id='a',message_id=key,event_token=str(seq))
            for seq,key in [(11,'2'),(12,'1'),(13,'2'),(14,'gone')]]
        rows=[dict(source='telegram',chat_id='a',message_id=key,text='synthetic') for key in ('2','1')]
        source.get_many.return_value=rows+[None]
        store=Mock()
        store.state.return_value={'backfill_done':True,'cursor':10,'change_seq':10}
        self.assertTrue(ingest_step(store,source))
        source.get_many.assert_called_once_with([('a','2'),('a','1'),('a','gone')])
        source.get.assert_not_called()
        store.apply.assert_called_once_with(rows,'telegram',deleted=[('a','gone')],change_seq=14,change_token='14')

    def test_source_page_error_never_applies_partial_data_or_checkpoint(self):
        import sqlite3
        from worker import ingest_step
        source=Mock(kind='telegram')
        source.changes.return_value=[dict(seq=11,chat_id='a',message_id='1',event_token='new')]
        source.get_many.side_effect=sqlite3.OperationalError('interrupted')
        store=Mock()
        store.state.return_value={'backfill_done':True,'cursor':10,'change_seq':10}
        with self.assertRaises(sqlite3.OperationalError):
            ingest_step(store,source)
        store.apply.assert_not_called()

    def test_source_page_length_mismatch_cannot_advance_checkpoint(self):
        from worker import ingest_step
        source=Mock(kind='telegram')
        source.changes.return_value=[dict(seq=11,chat_id='a',message_id='1',event_token='new')]
        source.get_many.return_value=[]
        store=Mock()
        store.state.return_value={'backfill_done':True,'cursor':10,'change_seq':10}
        with self.assertRaises(ValueError):
            ingest_step(store,source)
        store.apply.assert_not_called()

    def test_embedding_failure_does_not_publish_success(self):
        from worker import embedding_step
        store=Mock()
        store.pending.return_value=[dict(hash='a',text='synthetic',attempts=0)]
        model=Mock()
        model.embed.side_effect=TimeoutError('synthetic')
        self.assertFalse(embedding_step(store,model))
        store.save_embeddings.assert_not_called()
        store.embedding_failed.assert_called_once()

    @patch("worker.time.monotonic", return_value=10.0)
    def test_source_statistics_failure_never_blocks_message_ingestion(self, _clock):
        from worker import refresh_source_stats
        source=Mock(kind='telegram')
        source.stats.side_effect=TimeoutError('synthetic slow aggregate')
        store=Mock()
        refresh={}
        refresh_source_stats(store,source,refresh)
        self.assertIn('telegram',refresh)
        store.apply.assert_not_called()
        refresh_source_stats(store,source,refresh)
        source.stats.assert_called_once()

    def test_hybrid_keeps_keyword_results_during_model_failure(self):
        from server import search
        store=Mock()
        store.lexical.return_value=[dict(id=1,text='synthetic')]
        model=Mock()
        model.embed.side_effect=TimeoutError('synthetic')
        with patch('search_data.SearchData.authorized', side_effect=lambda rows, **kwargs: rows):
            result=search(store,model,dict(source='telegram',query='synthetic'))
        self.assertTrue(result['degraded'])
        self.assertEqual(len(result['results']),1)
        store.semantic.assert_not_called()


if __name__=='__main__':
    unittest.main()
