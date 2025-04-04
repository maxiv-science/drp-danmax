import logging
import os
import tempfile
import json
from datetime import datetime, timedelta, timezone

# for debugging
import io
import traceback

import zmq
from dranspose.event import EventData
from dranspose.parameters import BoolParameter, IntParameter, StrParameter
from dranspose.middlewares.stream1 import parse
from dranspose.data.stream1 import Stream1Data, Stream1End, Stream1Start
from dranspose.data.positioncap import PositionCapStart, PositionCapValues


from dranspose.middlewares.sardana import parse as sardana_parse
from dranspose.middlewares.positioncap import PositioncapParser
import numpy as np
from numpy import unravel_index
from scipy import ndimage

from bitshuffle import decompress_lz4
import azint
import fabio

logger = logging.getLogger(__name__)


class TomoWorker:
    @staticmethod
    def describe_parameters():
        params = [
            BoolParameter(name="tomo_repub", default=True),
            # FileParameter(name="poni"),
            IntParameter(name="radial_bins", default=1000),
            IntParameter(name="n_splitting", default=4),
            StrParameter(name="unit", default="q"),
            # FileParameter(name="mask"),
        ]
        return params

    def __init__(self, parameters, context, **kwargs):
        self.pile = None
        self.nimages = 0
        self.sock = None
        self.pcap = PositioncapParser()
        self.arm_time = None

        self.ai = None
        if "poni_file" in parameters:
            print("par", parameters["poni_file"])
            with tempfile.NamedTemporaryFile() as fp:
                fp.write(parameters["poni_file"].data)
                fp.flush()
                config = {}
                if "mask" in parameters and parameters["mask"].value != "":
                    with tempfile.NamedTemporaryFile() as maskfp:
                        ending = os.path.splitext(parameters["mask"].value)[1]
                        maskfp.write(parameters["mask_file"].data)
                        if ending == ".npy":
                            config["mask"] = np.load(maskfp.name)
                        else:
                            config["mask"] = fabio.open(maskfp.name).data
                        maskfp.flush()

                self.ai = azint.AzimuthalIntegrator(
                    fp.name,
                    parameters["n_splitting"].value,
                    parameters["radial_bins"].value,
                    unit=parameters["unit"].value,
                    **config,
                )

        if "tomo_repub" not in parameters or (
            "tomo_repub" in parameters and parameters["tomo_repub"].value is True
        ):
            if "context" not in context:
                logger.info("open context because there was none")
                context["context"] = zmq.Context()
                logger.info("binding PUSH socket with keepalive")
                context["socket"] = context["context"].socket(zmq.PUSH)
                context["socket"].setsockopt(zmq.TCP_KEEPALIVE, 1)
                context["socket"].setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)
                context["socket"].setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
                context["socket"].setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
                # hack to extract port from worker name
                context["socket"].bind("tcp://*:5556")
            else:
                logger.info("context is already there")

            self.sock = context["socket"]

    def _proc_basler(self, event):
        dat = parse(event.streams["basler5"])
        print(dat)
        if isinstance(dat, Stream1Data):
            # np.save("test.npy", dat.data)
            intens = dat.data.mean()
            print("mean:", intens)
            cg = ndimage.center_of_mass(dat.data)
            print("center:", cg)
            return {"basler_mean": intens, "basler_cg": cg}

    def _proc_pilatus(self, event, sardana):
        if self.ai is not None:
            data = parse(event.streams["pilatus"])
            if isinstance(data, Stream1Start):
                return {"azint": {"axis": self.ai.radial_axis}}
            if isinstance(data, Stream1Data):
                if "bslz4" in data.compression:
                    bufframe = event.streams["pilatus"].frames[1]
                    if isinstance(bufframe, zmq.Frame):
                        bufframe = bufframe.bytes
                    img = decompress_lz4(bufframe, data.shape, dtype=data.type)
                    # print("decomp", img, img.shape)
                    I, _ = self.ai.integrate(img)
                    logger.debug("got I %s", I.shape)
                    pos = {}
                    if hasattr(sardana, "pd_sam_x") and hasattr(sardana, "pd_sam_y"):
                        print("got position")
                        pos = {"x": sardana.pd_sam_x, "y": sardana.pd_sam_y}
                    return {"azint": {"I": I, "position": pos}}

    def process_event(self, event: EventData, parameters=None, *args, **kwargs):
        sardana = None
        if "sardana" in event.streams:
            sardana = sardana_parse(event.streams["sardana"])
            print("saranda is", sardana)

        if "basler5" in event.streams:
            return self._proc_basler(event)

        if "pilatus" in event.streams:
            logger.debug("use pilatus data for azint")
            return self._proc_pilatus(event, sardana)

        degree_to_enc_formula = np.poly1d([11930463, 0])
        angle = None
        triggerstr = None
        if "pcap_rot" in event.streams:
            res = self.pcap.parse(event.streams["pcap_rot"])
            if isinstance(res, PositionCapStart):
                self.arm_time = res.arm_time
            elif isinstance(res, PositionCapValues):
                triggertime = timedelta(seconds=res.fields["PCAP.TS_TRIG.Value"].value)
                d = res.fields["SFP3_SYNC_IN.POS1.Mean"].value
                angle = np.roots(degree_to_enc_formula - d)[0] - 50
                triggerstr = (self.arm_time + triggertime).isoformat()
        dat = None
        if "orca" in event.streams:
            dat = parse(event.streams["orca"])
            if self.sock:
                try:
                    if angle is not None or triggerstr is not None:
                        frame0 = event.streams["orca"].frames[0]
                        if isinstance(frame0, zmq.Frame):
                            frame0 = frame0.bytes
                        header = json.loads(frame0)
                        if angle is not None:
                            header["encoder_angle"] = angle
                        if triggerstr is not None:
                            header["timestamp_pcap"] = triggerstr
                        header["timestamp_worker"] = datetime.now(
                            timezone.utc
                        ).isoformat()
                        # parts = [json.dumps(header).encode()]+event.streams["orca"].frames[1:]
                        self.sock.send_json(
                            header,
                            flags=zmq.SNDMORE | zmq.NOBLOCK,
                        )
                        self.sock.send(
                            event.streams["orca"].frames[1], flags=zmq.NOBLOCK
                        )
                        # logger.info("send augmented header %s", header)
                        # self.sock.send_multipart(parts, flags=zmq.NOBLOCK)
                    else:
                        logger.info("no angle added")
                        self.sock.send_multipart(
                            event.streams["orca"].frames, flags=zmq.NOBLOCK
                        )
                except Exception as e:
                    logger.warning("cannot repub frame %s", e.__repr__())
                    # print more info
                    with io.StringIO() as output:
                        traceback.print_exc(file=output)
                        logger.warning(
                            "cannot repub frame (traceback): " + output.getvalue()
                        )

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
