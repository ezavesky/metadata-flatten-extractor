#! python
# ===============LICENSE_START=======================================================
# metadata-flatten-extractor Commons Clause Apache-2.0
# ===================================================================================
# Copyright (C) 2017-2020 AT&T Intellectual Property. All rights reserved.
# ===================================================================================
# This software file is distributed by AT&T 
# under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0; https://commonsclause.com/
#
# This file is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============LICENSE_END=========================================================
# -*- coding: utf-8 -*-

# Imports
from pathlib import Path
import io

from threading import Thread as Proc
# from multiprocessing import Process as Proc
import multiprocessing as mp
from queue import Empty
import collections
import atexit

Msg = collections.namedtuple('Msg', ['event', 'args'])

class BaseProcess(Proc):
    """A process backed by an internal queue for simple one-way message passing.
    """
    def __init__(self, *args, recv=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = mp.Queue()
        self._term = mp.Event()
        self._busy = mp.Event()
        self._recv = recv
        self._busy.clear()

    def send(self, event, *args):
        """Puts the event and args as a `Msg` on the queue
        """
        msg = Msg(event, args)
        self._queue.put(msg)

    def dispatch(self, msg):
        event, args = msg
        self._busy.set()
        handler = getattr(self, "do_%s" % event, None)
        if not handler:
            self._busy.clear()
            raise NotImplementedError("Process has no handler for [%s]" % event)
        ret_val = handler(*args)
        self.cascade(event, ret_val)
        self._busy.clear()
        return ret_val

    def cascade(self, event, ret_val):
        if ret_val is not None and self._recv is not None:
            self._recv.send(event, ret_val)

    def run_local(self, wait_queue=False):
        result = None
        msg = None
        try:
            msg = self._queue.get(timeout=5 if wait_queue else 0.5)
            result = self.dispatch(msg)
        except mp.TimeoutError as e:
            pass
        except Empty as e:
            pass
        if msg is None:
            return None
        return Msg(msg.event, result)
    
    def stop(self):
        self._term.set()

    def run(self):
        while self.is_alive() and not self._term.is_set():
            self.run_local(wait_queue=True)

    def start(self):
        atexit.register(self.stop)
        super().start()

    def busy(self):
        return not self._queue.empty() or self._busy.is_set()


### --- testing process --- 

def main():
    class MyProcess2(BaseProcess):
        def do_helloworld(self, *args):
            print("BASE2", args)
            return None


    class MyProcess1(BaseProcess):
        def do_helloworld(self, arg1, arg2):
            print("BASE1", arg1, arg2)
            return "---".join([arg1, arg2])

    print("MAIN")
    process2 = MyProcess2()
    process1 = MyProcess1(recv=process2)
    process1.start()
    process1.send('helloworld', 'hello', 'world')
    process2.run_local()
    process2.run_local()
    process1.send('helloworld', 'hello2', 'world2')
    process2.run_local()
    process1.stop()
    process1.join()


if __name__ == "__main__":
    main()