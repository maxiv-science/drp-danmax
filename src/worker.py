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
from scipy import ndimage

logger = logging.getLogger(__name__)


class TomoWorker:

    @staticmethod
    def describe_parameters():
        params = [
            BoolParameter(name="tomo_repub", default=True),
        ]
        return params

    def __init__(self, parameters, context, **kwargs):
        self.pile = None
        self.nimages = 0
        self.sock = None
        if "tomo_repub" in parameters and parameters["tomo_repub"].value is True:
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

        if "basler5" in event.streams:
            dat = parse(event.streams["basler5"])
            print(dat)
            if isinstance(dat, Stream1Data):
                #np.save("test.npy", dat.data)
                intens = dat.data.mean()
                print("mean:", intens)
                cg = ndimage.center_of_mass(dat.data)
                print("center:", cg)
                return {"basler_mean": intens, "basler_cg": cg}

        degree_to_enc_formula = np.poly1d([11930463,0])
        angle = None
        if "pcap_rot" in event.streams:
            rot = (event.streams["pcap_rot"].frames[0].bytes.decode().split(" ")[-1])
            try:
                d = float(rot)
                angle = np.roots(degree_to_enc_formula - d)[0] - 50
            except:
                pass
        dat = None
        if "orca" in event.streams:
            dat = parse(event.streams["orca"])

            if self.sock:
                try:
                    if angle is not None:
                        header = json.loads(event.streams["orca"].frames[0].bytes)
                        header["encoder_angle"] = angle
                        #parts = [json.dumps(header).encode()]+event.streams["orca"].frames[1:]
                        self.sock.send_json(header,
                            flags=zmq.SNDMORE|zmq.NOBLOCK,
                        )
                        self.sock.send(event.streams["orca"].frames[1], flags=zmq.NOBLOCK)
                        #logger.info("send augmented header %s", header)
                        #self.sock.send_multipart(parts, flags=zmq.NOBLOCK)
                    else:
                        logger.info("no angle added")
                        self.sock.send_multipart(event.streams["orca"].frames, flags=zmq.NOBLOCK)
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
