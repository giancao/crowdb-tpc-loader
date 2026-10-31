"""Worker checkpoints acknowledged only after the calling thread persists them."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from queue import Empty, Queue
from threading import Event


def journal_workers(items, worker, save, workers):
    messages = Queue()

    def execute(name, value):
        def checkpoint(state):
            ready, errors = Event(), []
            messages.put((name, deepcopy(state), ready, errors))
            ready.wait()
            if errors:
                raise errors[0]

        return worker(name, value, checkpoint)

    errors, interrupted = [], None
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = set()
        try:
            for name, value in items:
                pending.add(pool.submit(execute, name, value))
        except BaseException as exc:
            interrupted = exc
        while pending:
            message = None
            try:
                message = messages.get(timeout=0.05)
                name, state, ready, failures = message
                if interrupted is not None:
                    raise interrupted
                save(name, state)
                ready.set()
            except Empty:
                pass
            except BaseException as exc:
                interrupted = exc
                if message is not None:
                    failures.append(exc)
                    ready.set()
            try:
                for future in tuple(pending):
                    if future.done():
                        pending.remove(future)
                        error = future.exception()
                        if error is not None:
                            errors.append(error)
            except BaseException as exc:
                interrupted = exc
    if interrupted is not None:
        raise interrupted
    if errors:
        raise errors[0]
