import logging
import tempfile
import json

from dranspose.event import EventData
from dranspose.parameters import IntParameter, StrParameter
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
        ]
        return params

    def __init__(self, parameters=None, context=None, **kwargs):
        self.pile = None
        self.nimages = 0

    def process_event(self, event: EventData, parameters=None):
        sardana = None
        if "sardana" in event.streams:
            sardana = sardana_parse(event.streams["sardana"])

        dat = None
        if "orca" in event.streams:
            dat = parse(event.streams["orca"])
        if "andor3_balor" in event.streams:
            dat = parse(event.streams["andor3_balor"])

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
