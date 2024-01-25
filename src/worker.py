import logging
import tempfile
import json

from dranspose.event import EventData
from dranspose.parameters import IntParameter, StrParameter
from dranspose.middlewares.stream1 import parse
from dranspose.data.stream1 import Stream1Data
import numpy as np
from numpy import unravel_index

logger = logging.getLogger(__name__)


class TomoWorker:

    @staticmethod
    def describe_parameters():
        params = [
            IntParameter(name="test", default=0),
        ]
        return params

    def __init__(self, parameters=None, context=None, **kwargs):
        pass

    def process_event(self, event: EventData, parameters=None):
        pass


    def finish(self, parameters=None):
        print("finished")
