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
import os
import cv2 as cv
import numpy as np
import logging as log
import time 
import datetime
from openvino.inference_engine import IENetwork, IEPlugin
from scipy.spatial import distance as dist
from collections import OrderedDict
from RedisQueue import RedisQueue

class EmotionRecognition(object):
    
    def __init__(self, id, args):
        self._id = id
        self.cur_request_id = 0
        self.next_request_id = 1
        self.numObjects = 0
        self.net = IENetwork()
        self.input_blob = 1
        self.out_blob = 1
        self.exec_net = 1
        self.plugin = 1
        
        self.obj_type = "Emption"
        self.cv2 = cv
        self.input_stream = args.input
        self.prob_threshold = args.prob_threshold_fd
        self.model_xml= args.model_er
                
        self._load_model(self.model_xml, args)
        self.cv2.setUseOptimized(True)
    
    def _load_model(self, model_xml, args):
    
        # Step 1:  Loading all the model.
        # Vehicle detection
        model_bin = os.path.splitext(model_xml)[0] + ".bin"
    
        # Step 2: Initialize the Plugin ( MYRIAD )
        # This will initialize the myriad device..
        self.plugin = IEPlugin("MYRIAD", plugin_dirs="")
        
        # Step 3: Initialiaze the network model.     
        self.net = IENetwork(model=model_xml, weights=model_bin)    
        
        log.info("EmotionRecognition : Net Loaded.")
        log.info( self.net.layers)
    
        # Step 3.1: Prepare the input area
        log.info("EmotionRecognition : Preparing input blobs Det, Attr, Read {}".format(self.net.inputs))
        
        self.input_blob = next(iter(self.net.inputs))
        
        # Steo 3.2: Prepare the output area
        log.info("EmotionRecognition : Preparing output blobs")
        self.out_blob = next(iter(self.net.outputs))
        
        # Step 3.3: Set the batch size of the network.  
        # Reset these to 1
        self.net.batch_size = 1
        
        # Step 3.4 Read and pre-process input images       
        log.info("EmotionRecognition : Read Net  inputs {} ".format(self.net.inputs[self.input_blob].shape) )
        self.n, self.c, self.h, self.w = self.net.inputs[self.input_blob].shape
    
        # Step 4
        log.info("EmotionRecognition :  Loading model to the plugin")
        self.exec_net = self.plugin.load(network=self.net)
        

    def getEmotion(self, frame):    
                
        inf_start = time.time()
        in_frame = self.cv2.resize(frame, (self.w, self.h))
        in_frame = in_frame.transpose((2, 0, 1))  # Change data layout from HWC to CHW
        in_frame = in_frame.reshape((self.n, self.c, self.h, self.w))
        self.exec_net.start_async(request_id=0, inputs={self.input_blob: in_frame})
        index = 0
    
        if self.exec_net.requests[0].wait(-1) == 0:
            inf_end = time.time()
            det_time = inf_end - inf_start

            # Parse detection results of the current request
            res = self.exec_net.requests[0].outputs[self.out_blob]           
                        
            vals=[[0,res[0][0][0][0]],[1,res[0][1][0][0]],[2,res[0][2][0][0]],[3,res[0][3][0][0]],[4,res[0][4][0][0]]]
            log.info("EmotionEngine: Potential values are {}".format(vals))

            index, value = max(vals, key=lambda item: item[1])   
            log.info("EmotionEngine: Index, Value {},{}".format(index,value))
            
        return index
        