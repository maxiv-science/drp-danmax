import logging
from pathlib import Path

from dranspose.event import ResultData
from dranspose.parameters import StrParameter, BoolParameter
import os
import h5py
import numpy as np
import h5pyd
from copy import copy


logger = logging.getLogger(__name__)


class TomoReducer:
    @staticmethod
    def describe_parameters():
        params = [
            BoolParameter(name="pileup", default=False),
        ]
        return params

    def __init__(self, parameters=None, **kwargs):
        self.pile = None
        self.nimages = 0
        self.filename = None
        self.publish = {
            "basler": {"mean": [], "cg": [0, 0]},
            "azint": {"positions": {}, "I": []},
        }

    def process_result(self, result: ResultData, parameters=None):
        if result.payload:
            if "azint" in result.payload:
                if "axis" in result.payload["azint"]:
                    self.publish["azint"]["radial_axis"] = result.payload["azint"][
                        "axis"
                    ]
                else:
                    self.publish["azint"]["I"].append(result.payload["azint"]["I"])
                    for k, v in result.payload["azint"]["position"].items():
                        if k not in self.publish["azint"]["positions"]:
                            self.publish["azint"]["positions"][k] = []
                        self.publish["azint"]["positions"][k].append(v)

            if "basler_mean" in result.payload:
                self.publish["basler"]["mean"] = result.payload["basler_mean"]
            if "basler_cg" in result.payload:
                self.publish["basler"]["cg"] = result.payload["basler_cg"]
            if "filename" in result.payload and result.payload["filename"] != "":
                fn = result.payload["filename"]
                parts = fn.split(".")
                self.filename = f"{'.'.join(parts[:-1])}_mean.{parts[-1]}"
                logger.info("write to %s", self.filename)
            if "pile" in result.payload:
                if self.pile is None:
                    self.pile = result.payload["pile"]
                else:
                    self.pile += result.payload["pile"]
                self.nimages += result.payload["nimages"]

    def finish(self, parameters=None):
        if self.pile is not None:
            Path(self.filename).parent.mkdir(parents=True, exist_ok=True)
            if os.path.isfile(self.filename):
                logger.error("file exists already, not saving")
                return
            logger.info("using pileup file %s", Path(self.filename))
            with h5py.File(self.filename, "w") as fh:
                group = fh.create_group("pileup")
                group.create_dataset("nimages", data=self.nimages)
                group.create_dataset(
                    "data", data=(self.pile / self.nimages).astype(np.float32)
                )
