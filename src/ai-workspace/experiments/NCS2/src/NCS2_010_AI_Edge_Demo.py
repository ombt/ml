#!/usr/bin/env python
"""
 Copyright (c) 2018 Sandeep Sachdeva

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
from multiprocessing import Process, Queue
import threading
import os
import sys
import argparse
import logging as log
import cv2
from openvino.inference_engine import IENetwork, IEPlugin
import numpy as np
import queue
from argparse import ArgumentParser
import logging as log
import time
from tensorflow.contrib.signal.python.ops.shape_ops import frame

def build_argparser():
    parser = ArgumentParser()
    parser.add_argument("-m_va", "--model_va", 
                        help="Path to a vehicle detection .xml file with a trained model.", required=True, type=str)
    
    parser.add_argument("-i_va", "--input_va",
                        help="Path to vehicle video file or image. 'cam' for capturing video stream from camera", required=True,
                        type=str)
    
    #parser.add_argument("-m_pa", "--model_pa", 
    #                   help="Path to a person attributes .xml file with a trained model.", required=True, type=str)

    #parser.add_argument("-i_pa", "--input_pa",
    #                    help="Path to person video file or image. 'cam' for capturing video stream from camera", required=True,
    #                    type=str)
    
    parser.add_argument("-d", "--device",
                        help="Specify the target device to infer on; CPU, GPU, FPGA or MYRIAD is acceptable. Demo "
                             "will look for a suitable plugin for device specified (CPU by default)", default="CPU",
                        type=str)
    parser.add_argument("-pt", "--prob_threshold", help="Probability threshold for detections filtering",
                        default=0.5, type=float)

    return parser     


def inference_job_async(job_queue, ncs_classifer):
    while True:
        frame = job_queue.get()
        if type(frame) != np.ndarray:
            job_queue.put(None)
            break
        
        # Hard coded for now.
        # TODO - Need to change this.
        w = 672
        h = 384
        
        in_frame = cv2.resize(frame, (w, h))
        in_frame = in_frame.transpose((2, 0, 1))  # Change data layout from HWC to CHW
        in_frame = in_frame.reshape((1, 3, h, w))

        res = ncs_classifer.predict_async(in_frame)
             
        for obj in res[0][0]:
                # Draw only objects when probability more than specified threshold
                                    
                if obj[2] > args.prob_threshold:
                    xmin = int(obj[3] * w)
                    ymin = int(obj[4] * h)
                    xmax = int(obj[5] * w)
                    ymax = int(obj[6] * h)
                    class_id = int(obj[1])
                    
                    # Highlight Green box.
                    color = (10, 200, 50)
                    cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), color, 2)
        
       
        cv2.imshow("Detection Results ", frame)
   
        job_queue.task_done()
        

def get_frames_job(job_queue, image_file):

    input_stream = image_file    
    assert os.path.isfile(image_file), "Specified input file doesn't exist"

    cap = cv2.VideoCapture(input_stream)    
    
    while cap.isOpened():        
        ret, frame = cap.read()
        if not ret:
            break        
        job_queue.put(frame)
    
    # put none to indicate end of queue
    job_queue.put(None)


class NcsVehicleTracker(object):
    def __init__(self, id, queue, model_xml):
        self._id = id
        self.current_request_id = 0
        self.next_request_id = 1
        self._queue = queue
        self.numObjects = 0
        self.initial_w = 960
        self.initial_h = 540
        self._load_model(model_xml)
        
    
    def _load_model(self, model_xml):
        model_bin = os.path.splitext(model_xml)[0] + ".bin"

        # Load plugin for NCS2
        log.info("NCSVehicleTracker: Initiating Intel NCS2")
        self.plugin = IEPlugin(device='MYRIAD')
        
        log.info("NCSVehicleTracker: Initiate net. Model Files:\n\t{}\n\t{}".format(model_xml, model_bin))
        self.net = IENetwork(model=model_xml, weights=model_bin)
        
        self.net.batch_size = 1
        
        self.exec_net = self.plugin.load(network=self.net, num_requests=2)
        log.info("NCSVehicleTracker: Net Loaded")

    
    def predict(self, image):

        log.info("NCSVehicleTracker: predict: Set input and output Blobs")
        input_blob = next(iter(self.net.inputs))
        out_blob = next(iter(self.net.outputs))

        log.info("NCSVehicleTracker: predict: Predicting")
        res = self.exec_net.infer(inputs={input_blob: image})

        # get result back
        output = res[out_blob]
        log.info("NCSVehicleTracker: predict: Decoding results")



        probs = np.squeeze(output[0])
        top_ind = np.argsort(probs)[-1:][::-1]
        return top_ind


    def predict_async(self, image):

        log.info("NCSVehicleTracker: predict_async: Set input and output Blobs")
        input_blob = next(iter(self.net.inputs))
        out_blob = next(iter(self.net.outputs))

        log.info("NCSVehicleTracker: predict_async: Start async requests")
        self.exec_net.start_async(request_id=self.next_request_id,
                                  inputs={input_blob: image})

        if self.exec_net.requests[self.current_request_id].wait(-1) == 0:
            res = self.exec_net.requests[self.current_request_id].outputs[out_blob]
                        
            # exchange request id
            self.current_request_id, self.next_request_id = self.next_request_id, self.current_request_id
        
            return res


class Scheduler:
    def __init__(self, deviceids, model_xml):
        self._queue = queue.Queue()
        self._ids = deviceids
        self.__init_workers(model_xml)

    def __init_workers(self, model_xml):
        self._workers = list()
        for _id in self._ids:
            self._workers.append(NcsVehicleTracker(_id, self._queue, model_xml))

    def start(self, video_va_file):

        start_time = time.time()
        # start the workers
        threads = []

        # add producer thread for image pre-processing
        producer_thread = threading.Thread(target=get_frames_job, args=(self._queue, video_va_file))
        producer_thread.start()
        threads.append(producer_thread)

        # schedule workers
        for worker in self._workers:
            thworker = threading.Thread(target=inference_job_async, args=(self._queue, worker))
            thworker.start()
            threads.append(thworker)

        # wait all fo workers finish
        for _thread in threads:
            _thread.join()

        end_time = time.time()
        print("all of workers have been done within ", end_time - start_time)


def run(model_va, model_pa, video_va, video_pa, device_ids):
    
    # init scheduler
    x = Scheduler(device_ids, model_va)

    # start processing and wait for complete
    x.start(video_va)


if __name__ == "__main__":
    log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.DEBUG, stream=sys.stdout)
    args = build_argparser().parse_args()

    device_ids = 1
 
    # run(args.imgpath, device_ids, args.model, input_shape)
    # Run to include -
    #      va model
    #   -  pa_model
    #   -  i_va - the vehicle video file.
    #   -  i_pa - the person video file.
    #   - num_devices
    
    run(args.model_va, "", args.input_va, "", args.device)



