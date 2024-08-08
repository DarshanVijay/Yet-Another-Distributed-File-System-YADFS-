import os
import queue
import threading
import traceback



class Server(threading.Thread):
    """
    The base class of NameServer and DataServer.
    """
    def __init__(self, root_path: str):
        super().__init__()
        self.root_path = root_path
        os.makedirs(self.root_path, exist_ok=True)
        
        # server states
        self.is_running = True
        
        # received command channel
        self.cmd_chan = queue.Queue(maxsize=1)
        
        # data channels
        self.in_chan = queue.Queue()
        self.out_chan = queue.Queue()
        
        # commands execution dict (should be detailedly defined in derived classes)
        self.exec = {}

    def shutdown(self):
        self.is_running = False
        
    def run(self):
        # Start the heartbeat thread
        while self.is_running:
            # print(self, 'is ready for cmd')
            argv = self.cmd_chan.get().split()
            cmd = argv[0]
            args = argv[1:]
            try:
                # print(self, 'execute', cmd, args)
                self.exec[cmd](*args)
            except:
                traceback.print_exc()
            
   