import json
import logging
import threading
from glob import glob

import zmq
from dranspose.replay import replay


# not yet working as the client connects too slowly. replay should wait before it starts replaying the file to allow th clients to connect to the zmq socket.
def consume_repub(num: int) -> int:
    ctx = zmq.Context()
    s = ctx.socket(zmq.PULL)
    s.connect("tcp://127.0.0.1:5556")
    logging.info("started consumer")
    for _ in range(num):
        data = s.recv_multipart(copy=False)
        logging.info("received data %s", data)

    ctx.close()
    return num


def test_repub(tmp_path):
    stop_event = threading.Event()
    done_event = threading.Event()

    param_file = tmp_path / "param.json"
    with open(param_file, "w") as f:
        json.dump([{"name": "tomo_repub", "data": "True"}], f)

    thread = threading.Thread(
        target=replay,
        args=(
            "src.worker:TomoWorker",
            "src.reducer:TomoReducer",
            list(glob("data/*245334e26eae.cbors")),
            None,
            param_file,
        ),
        kwargs={
            "port": 5010,
            "nworkers": 1,
            "stop_event": stop_event,
            "done_event": done_event,
        },
    )
    thread.start()
    # do live queries

    done_event.wait()

    stop_event.set()

    thread.join()
