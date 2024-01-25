import logging

from dranspose.event import ResultData
from dranspose.parameters import StrParameter, BoolParameter
import os
import h5py
import numpy as np

logger = logging.getLogger(__name__)

class TomoReducer:

    def __init__(self, parameters=None, **kwargs):
        pass

    def process_result(self, result: ResultData, parameters=None):
        pass

    def finish(self, parameters=None):
        pass