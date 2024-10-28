import logging
import threading
from glob import glob

import h5pyd
from dranspose.replay import replay


def test_twodmapping():
    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "src.worker:TomoWorker",
            "src.reducer:TomoReducer",
            list(glob("data/*7aba6cdc99a9.cbors")),
            None,
            "parameters.json",
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    # do live queries

    done_event.wait()

    f = h5pyd.File("http://localhost:5010/", "r")
    logging.info("file %s", list(f.keys()))
    logging.info("azint %s", list(f["azint"].keys()))
    assert set(f["azint/positions"].keys()) == {"x", "y"}
    logging.info("I %s", f["azint/I"])
    assert f["azint/I"].shape == (50, 1000)
    stop_event.set()

    thread.join()
