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
from argparse import ArgumentParser
import queue
import logging as log
import time
import threading
from plainbox.impl.integration_tests import execute_job
#import objTracker
from imutils.video import FileVideoStream
from imutils.video import FPS
import imutils
#import NCS2_016_ThreadedObjTracker as objTracker
import NCS2_024_ThreadedFaceDetection as objTracker
import NCS2_025_ThreadedEmotionRecognition as emotionRec


# Will continue to use this as a standard for now.

def build_argparser():
    parser = ArgumentParser()

    parser.add_argument("-m_fd", "--model_fd", 
                        help="Path to a face detection .xml file with a trained model.", required=True, type=str)
    parser.add_argument("-i", "--input",
                        help="Path to video file.", required=True, type=str)

    parser.add_argument("-m_er", "--model_er", 
                        help="Path to a emotion recognition .xml file with a trained model.", required=False, type=str)
    
    parser.add_argument("-d", "--device",
                        help="Specify the target device to infer on; CPU, GPU, FPGA or MYRIAD is acceptable. Demo "
                             "will look for a suitable plugin for device specified (CPU by default)", default="CPU",
                        type=str)
    parser.add_argument("-pt_fd", "--prob_threshold_fd", help="Probability threshold for face detections filtering",
                        default=0.5, type=float)
    parser.add_argument("-pt_er", "--prob_threshold_er", help="Probability threshold for emotion recognition filtering",
                        default=0.5, type=float)
    
    return parser     

def execute_all_models(tracker, threaded, args):
    
    log.info("ExecuteAllModels : Threaded is {}".format(threaded))
    
    if threaded :
        tracker.executeModelThreaded(args)
    else :
        tracker.executeModel(args)

class Scheduler:
    def __init__(self, fvs, Threaded, args):
        self._queue = queue.Queue()
        self._ids = args.device
        self.__init_workers(fvs, args)
        self.threaded = Threaded

    def __init_workers(self, fvs, args):
        self._workers = list()
        
        # TODO Make this dynamic later.
        # 0 for Vehicles 
        ncs2_ER = emotionRec.EmotionRecognition(1,args)
        ncs2_FD = objTracker.FaceDetection(0, ncs2_ER, fvs, args)
        self._workers.append(ncs2_FD)
        
        

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
    fvs = FileVideoStream(args.input)
    fvs.start()
     
    Threaded = True

    log.info("Main : Launching Scheduler")
     
    # init scheduler
    x = Scheduler(fvs, Threaded, args)

    # start processing and wait for complete
    x.start(args)
    
    fvs.stop()
    
    del fvs
    
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

    
