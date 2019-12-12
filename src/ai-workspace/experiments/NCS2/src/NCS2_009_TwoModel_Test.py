from multiprocessing import Process, Queue
import threading
import os
import argparse
import logging as log
import cv2
from time import time
from openvino.inference_engine import IENetwork, IEPlugin
import numpy as np
import queue


def inference_job_async(job_queue, ncs_classifer):
    while True:
        xfile = job_queue.get()
        if type(xfile) != np.ndarray:
            job_queue.put(None)
            break
        ncs_classifer.predict_async(xfile)
        job_queue.task_done()

def image_preprocess_job(job_queue, files, w, h):
    for mfile in files:
        image = cv2.imread(mfile)
        image = cv2.resize(image, (w, h))
        image = image.transpose((2, 0, 1))  # Change data layout from HWC to CHW
        image = image[np.newaxis, :, :, :]

        job_queue.put(image)

    # put none to indicate end of queue
    job_queue.put(None)

class NcsClassifier(object):
    def __init__(self, id, queue, model_xml):
        self._id = id
        self.current_request_id = 0
        self.next_request_id = 1
        self._queue = queue
        self._load_model(model_xml)
        self.load_labels("/home/s/openvino_models/models/imagenet1000_clsid_to_human.txt")
        
    def load_labels(self, filename):
        with open(filename, 'r') as f:
            self.labels_map = [x.split(sep=' ', maxsplit=1)[-1].strip() for x in f]

    def _load_model(self, model_xml):
        model_bin = os.path.splitext(model_xml)[0] + ".bin"

        # Plugin initialization for specified device and load extensions library if specified
        self.plugin = IEPlugin(device='MYRIAD')
        #self.plugin.set_config({"VPU_FORCE_RESET":"NO"})
        # Read IR
        log.info("Loading network files:\n\t{}\n\t{}".format(model_xml, model_bin))
        self.net = IENetwork(model=model_xml, weights=model_bin)
        self.net.batch_size = 1
        self.exec_net = self.plugin.load(network=self.net, num_requests=2)

    def predict(self, image):
        input_blob = next(iter(self.net.inputs))
        out_blob = next(iter(self.net.outputs))

        # do inference
        res = self.exec_net.infer(inputs={input_blob: image})

        # get result back
        output = res[out_blob]

        probs = np.squeeze(output[0])
        top_ind = np.argsort(probs)[-1:][::-1]
        return top_ind

    def predict_async(self, image):
        input_blob = next(iter(self.net.inputs))
        out_blob = next(iter(self.net.outputs))

        self.exec_net.start_async(request_id=self.next_request_id,
                                  inputs={input_blob: image})

        if self.exec_net.requests[self.current_request_id].wait(-1) == 0:
            res = self.exec_net.requests[self.current_request_id].outputs[out_blob]
            probs = np.squeeze(res)
            top_ind = np.argsort(probs)[-1:][::-1]
            det_label = "none"
            for ind in top_ind:
                det_label = self.labels_map[ind]
                # print("Found label {}".format(det_label))
                print("Worker id {}, predicted index and label {}".format(self._id, det_label))

        # exchange request id
        self.current_request_id, self.next_request_id = self.next_request_id, self.current_request_id


class Scheduler:
    def __init__(self, deviceids, model_xml):
        self._queue = queue.Queue()
        self._ids = deviceids
        self.__init_workers(model_xml)

    def __init_workers(self, model_xml):
        self._workers = list()
        for _id in self._ids:
            self._workers.append(NcsClassifier(_id, self._queue, model_xml))

    def start(self, xfilelst, input_shape):

        start_time = time()
        # start the workers
        threads = []

        n, c, h, w = input_shape

        # add producer thread for image pre-processing
        producer_thread = threading.Thread(target=image_preprocess_job, args=(self._queue, xfilelst, w, h))
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

        end_time = time()
        print("all of workers have been done within ", end_time - start_time)


def run(img_path, device_ids, model_xml, input_shape):
    # scan all files under img_path
    xlist = list()
    for xfile in os.listdir(img_path):
        xlist.append(os.path.join(img_path, xfile))

    # init scheduler
    x = Scheduler(device_ids, model_xml)

    # start processing and wait for complete
    x.start(xlist, input_shape)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--imgpath", help="path to your images to be proceed")
    parser.add_argument("--num_device", type=int, help="number of ncs2 device", default=1)
    parser.add_argument("--model", help="Path to an .xml file with a trained model.", required=True, type=str)
    parser.add_argument("--input_shape", help='input shape of model', type=str, required=True)

    args = parser.parse_args()

    device_ids = [int(x) for x in range(args.num_device)]
    input_shape =[int(x) for x in args.input_shape[1:-1].split(',')]

    print(input_shape)
    print(args.imgpath)
    print(device_ids)

    run(args.imgpath, device_ids, args.model, input_shape)
