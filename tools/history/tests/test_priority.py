import tempfile
import threading
import time
import unittest
from pathlib import Path


class PriorityTests(unittest.TestCase):
    def test_waiting_query_prevents_another_background_batch(self):
        from priority import model_slot, queries_waiting, EmbeddingBusy
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            acquired=threading.Event()
            def query():
                with model_slot(root,True,wait_seconds=2):
                    acquired.set()
            with model_slot(root,False):
                thread=threading.Thread(target=query);thread.start()
                deadline=time.monotonic()+1
                while not queries_waiting(root) and time.monotonic()<deadline:time.sleep(.01)
                self.assertTrue(queries_waiting(root))
                with self.assertRaises(EmbeddingBusy):
                    with model_slot(root,False):pass
                self.assertFalse(acquired.is_set())
            thread.join(timeout=2)
            self.assertTrue(acquired.is_set())
            self.assertFalse(queries_waiting(root))

    def test_timed_out_query_releases_priority(self):
        from priority import model_slot,queries_waiting
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with model_slot(root,False):
                with self.assertRaises(TimeoutError):
                    with model_slot(root,True,wait_seconds=.02):pass
            self.assertFalse(queries_waiting(root))

    def test_repeated_queries_share_cached_embedding(self):
        from embeddings import Embedder
        with tempfile.TemporaryDirectory() as directory:
            model=Embedder(Path(directory));model.expected_identity='fake';model.identity=lambda:'fake'
            calls=[]
            def request(texts):
                calls.append(texts);return [[1.0]+[0.0]*1023]
            model.request_vectors=request
            self.assertEqual(model.embed(['pregunta'],True),model.embed(['pregunta'],True))
            self.assertEqual(len(calls),1)


if __name__=='__main__':unittest.main()

class InteractiveBudgetTests(unittest.TestCase):
    def test_busy_model_respects_query_budget_without_starting_another_request(self):
        from embeddings import Embedder
        from priority import model_slot
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);model=Embedder(root)
            model.expected_identity='fake';model.identity=lambda:'fake'
            requests=[];model.request_vectors=lambda texts,**kw:requests.append(texts)
            error=None;begin=time.monotonic()
            with model_slot(root,False):
                try:model.embed(['synthetic query'],interactive=True,budget_seconds=.03)
                except Exception as exc:error=exc
            self.assertIsInstance(error,TimeoutError)
            self.assertLess(time.monotonic()-begin,.5)
            self.assertEqual(requests,[])
