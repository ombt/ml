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
from __future__ import print_function
import sys
import os
from argparse import ArgumentParser
import cv2
import numpy as np
import queue
import logging as log
import time
from openvino.inference_engine import IENetwork, IEPlugin
from scipy.spatial import distance as dist
from collections import OrderedDict
from caffe._caffe import Net
import threading
from plainbox.impl.integration_tests import execute_job
#import objTracker
import NCS2_016_ThreadedObjTracker as objTracker


# Will continue to use this as a standard for now.

def build_argparser():
    parser = ArgumentParser()

    parser.add_argument("-m_va", "--model_va", 
                        help="Path to a vehicle detection .xml file with a trained model.", required=True, type=str)
    parser.add_argument("-i_va", "--input_va",
                        help="Path to video file for vehicle", required=True, type=str)

    parser.add_argument("-m_pa", "--model_pa", 
                        help="Path to a person detection .xml file with a trained model.", required=False, type=str)
    parser.add_argument("-i_pa", "--input_pa",
                        help="Path to video file for Person", required=False, type=str)
    
    parser.add_argument("-mx_d_va", "--maxdisappeared_va",
                        help="How soon do you want to fade the count from the screen for Vehicles", required=True,
                        type=str)

    parser.add_argument("-mx_d_pa", "--maxdisappeared_pa",
                        help="How soon do you want to fade the count from the screen for Person", required=False,
                        type=str)
    
    parser.add_argument("-d", "--device",
                        help="Specify the target device to infer on; CPU, GPU, FPGA or MYRIAD is acceptable. Demo "
                             "will look for a suitable plugin for device specified (CPU by default)", default="CPU",
                        type=str)
    parser.add_argument("--labels", help="Labels mapping file", default=None, type=str)
    parser.add_argument("-pt_va", "--prob_threshold_va", help="Probability threshold for vehicle detections filtering",
                        default=0.5, type=float)
    parser.add_argument("-pt_pa", "--prob_threshold_pa", help="Probability threshold for Person detections filtering",
                        default=0.5, type=float)

    parser.add_argument("-pp", "--plugin_dir", help="Path to a plugin folder", type=str, default=None)

    parser.add_argument("-df", "--datafile", help="Data File ", required=False, type=str)
    
    return parser     

def execute_all_models(job_queue, tracker, args):
    tracker.executeModel(args)            

class Scheduler:
    def __init__(self, deviceids, model_xml, args):
        self._queue = queue.Queue()
        self._ids = deviceids
        self.__init_workers(args)

    def __init_workers(self, args):
        self._workers = list()
        
        # TODO Make this dynamic later.
        # 0 for Vehicles 
        ncs2_VT = objTracker.ObjectTracker(0, self._queue, args.model_va, args)
        self._workers.append(ncs2_VT)

        if int(self._ids) > 1:    
            # 1 for Person
            ncs2_PT = objTracker.ObjectTracker(1, self._queue, args.model_pa, args)
            self._workers.append(ncs2_PT)


    def start(self, args):

        log.info("Scheduler : start : Launching thread pool")
    
        start_time = time.time()
        # start the workers
        threads = []
        # schedule workers
        process1 = 0
        
        for worker in self._workers:
            if process1 == 0:
                process1 = 1
                continue
            
            thworker = threading.Thread(target=execute_all_models, args=(self._queue, worker, args))
            thworker.start()
            threads.append(thworker)
            process1 = 1
            
        # wait all fo workers finish
        for _thread in threads:
            _thread.join()

        end_time = time.time()
        print("all of workers have been done within ", end_time - start_time)


def run(model_va, model_pa, video_va, video_pa, device_ids, args):
    
    # init scheduler
    x = Scheduler(device_ids, model_va, args)

    # start processing and wait for complete
    x.start(args)


if __name__ == "__main__":
    log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.DEBUG, stream=sys.stdout)
    args = build_argparser().parse_args()
 
    # run(args.imgpath, device_ids, args.model, input_shape)
    # Run to include -
    #      va model
    #   -  pa_model
    #   -  i_va - the vehicle video file.
    #   -  i_pa - the person video file.
    #   - num_devices
    
    run(args.model_va, "", args.input_va, "", args.device, args)

    