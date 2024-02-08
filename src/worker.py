import logging
import tempfile
import json

import zmq
from dranspose.event import EventData
from dranspose.parameters import BoolParameter
from dranspose.middlewares.stream1 import parse
from dranspose.data.stream1 import Stream1Data, Stream1End, Stream1Start

from dranspose.middlewares.sardana import parse as sardana_parse
import numpy as np
from numpy import unravel_index

logger = logging.getLogger(__name__)


class TomoWorker:

    @staticmethod
    def describe_parameters():
        params = [
            BoolParameter(name="tomo_repub"),
        ]
        return params

    def __init__(self, parameters, context, **kwargs):
        self.pile = None
        self.nimages = 0
        self.sock = None
        if parameters["tomo_repub"].value is True:
            if "context" not in context:
                context["context"] = zmq.Context()
                context["socket"] = context["context"].socket(zmq.PUSH)
                # hack to extract port from worker name
                context["socket"].bind(f"tcp://*:5556")

            self.sock = context["socket"]

    def process_event(self, event: EventData, parameters=None):
        sardana = None
        if "sardana" in event.streams:
            sardana = sardana_parse(event.streams["sardana"])

        dat = None
        if "orca" in event.streams:
            dat = parse(event.streams["orca"])
            if self.sock:
                try:
                    self.sock.send_multipart(event.streams["orca"].frames)
                except Exception as e:
                    logger.warning("cannot repub frame %s", e.__repr__())

        if isinstance(dat, Stream1Start):
            return {"filename": dat.filename}

        elif isinstance(dat, Stream1Data):
            pileup = parameters["pileup"].value
            if pileup:
                if self.pile is None:
                    self.pile = np.zeros_like(dat.data, dtype=np.uint32)
                self.pile += dat.data
                self.nimages += 1
        elif isinstance(dat, Stream1End):
            pileup = parameters["pileup"].value
            print("send partial pileup")
            if pileup:
                return {"pile": self.pile, "nimages": self.nimages}

    def finish(self, parameters=None):
        print("finished")
