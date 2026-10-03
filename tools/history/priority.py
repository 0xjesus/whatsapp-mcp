"""Cross-process foreground priority without expiring a still-waiting query."""
import fcntl
import time
from contextlib import contextmanager


class EmbeddingBusy(RuntimeError):
    pass


@contextmanager
def query_priority(root):
    with (root/'query-waiters.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_SH)
        yield


def queries_waiting(root):
    with (root/'query-waiters.lock').open('a') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True


@contextmanager
def model_slot(root,interactive,wait_seconds=35):
    with (root/'query-waiters.lock').open('a') as indicator, (root/'embedding.lock').open('a') as lock:
        if interactive:
            fcntl.flock(indicator,fcntl.LOCK_SH)
        else:
            try:
                fcntl.flock(indicator,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                raise EmbeddingBusy('Foreground query waiting') from None
        deadline=time.monotonic()+wait_seconds
        while True:
            try:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not interactive:
                    raise EmbeddingBusy('Foreground model request running') from None
                if time.monotonic()>=deadline:
                    raise TimeoutError('Embedding queue busy')
                time.sleep(.02)
        if not interactive:
            # Queries can now announce themselves while this finite batch finishes.
            fcntl.flock(indicator,fcntl.LOCK_UN)
        yield
