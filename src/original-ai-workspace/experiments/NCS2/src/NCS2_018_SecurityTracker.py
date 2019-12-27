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
import multiprocessing as Process
from plainbox.impl.integration_tests import execute_job
#import objTracker
import NCS2_016_ThreadedObjTracker as objTracker
from imutils.video import FileVideoStream
from imutils.video import FPS
import imutils


# Will continue to use this as a standard for now.

def build_argparser():
    parser = ArgumentParser()

    parser.add_argument("-m", "--model", 
                        help="Path to detection .xml file with a trained model.", required=True, type=str)
    parser.add_argument("-i", "--input",
                        help="Path to video file.", required=True, type=str)

    parser.add_argument("-o", "--output", 
                        help="Path to the output data file.", required=True, type=str)
    parser.add_argument("-t", "--tracker",
                        help="Path to tracker file.", required=True, type=str)
    
    parser.add_argument("-mx_d", "--maxdisappeared",
                        help="How soon do you want to fade the count for the object", required=True,
                        type=str)
    
    parser.add_argument("-d", "--device",
                        help="Specify the num devices.", default=0, type=int)
    parser.add_argument("-pt", "--prob_threshold", help="Probability threshold for vehicle detections filtering",
                        default=0.5, type=float)
        
    return parser     

def execute_all_models(tracker, threaded, args):
    
    log.info("ExecuteAllModels : Threaded is {}".format(threaded))
    
    if threaded :
        tracker.executeModelThreaded(args)
    else :
        tracker.executeModel(args)

class Scheduler:
    def __init__(self, fvsV, fvsP, Threaded, args):
        self._queue = queue.Queue()
        self._ids = args.device
        self.__init_workers(fvsV, fvsP, args)
        self.threaded = Threaded

    def __init_workers(self, fvsV, fvsP, args):
        self._workers = list()
        
        # TODO Make this dynamic later.
        # 0 for Vehicles 
        ncs2_VT = objTracker.ObjectTracker(0, fvsV, args)
        self._workers.append(ncs2_VT)

        if int(self._ids) > 1:    
            # 1 for Person
            ncs2_PT = objTracker.ObjectTracker(1, fvsP, args)
            self._workers.append(ncs2_PT)


    def start(self, args):

        log.info("Scheduler : start : Launching thread pool")
    
        start_time = time.time()
        # start the workers
        threads = []
        # schedule workers
        process1 = 0
        
        for worker in self._workers:
            #if process1 == 0:
            #    process1 = 1
            #    continue
            
            thworker = threading.Thread(target=execute_all_models, args=( worker, self.threaded, args))
            thworker.start()
            threads.append(thworker)
            #process1 = 1
            
        # wait all fo workers finish
        for _thread in threads:
            _thread.join()

        end_time = time.time()
        print("all of workers have been done within ", end_time - start_time)


def run(args):
    
    # Perform minimum stuff here.
    # Open video files and pass the FVS.
    log.info("Main : starting video file thread...for Vehicles")
    fvsV = FileVideoStream(args.input_va)
    fvsV.start()
    time.sleep(1.0)

    log.info("Main : starting video file thread...for Persons")     
    fvsP = FileVideoStream(args.input_pa)
    fvsP.start()
    time.sleep(1.0)
     
    Threaded = False

    log.info("Main : Launching Scheduler")
     
    # init scheduler
    x = Scheduler(fvsV, fvsP, Threaded, args)

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
    
    run(args)

    