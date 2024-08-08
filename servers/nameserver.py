import random       
import os
import pickle
import threading
import time
from colorama import Fore
from .server import Server
from .dataserver import DataServer
from typing import List


class NameServer(Server):
    """
    Management of files and dataservers.
    """
    def __init__(self, root_path: str, dataservers: List[DataServer], heartbeat_timeout: int = 30):
        super().__init__(root_path=root_path)
        self.dataservers = dataservers
         # Heartbeat timeout in seconds
        self.heartbeat_timeout = heartbeat_timeout

        # Dictionary to store the last heartbeat time for each DataServer
        self.last_heartbeat_times = {ds: time.time() for ds in dataservers}

        # Thread for checking heartbeats
        heartbeat_thread = threading.Thread(target=self.check_heartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        self.file_tree_path = os.path.join(self.root_path, 'file_tree.pkl')
        if os.path.isfile(self.file_tree_path):
            # load
            with open(self.file_tree_path, 'rb') as f:
                self.file_tree = pickle.load(f)
        else:
            # init
            self.file_tree = {'.files': set()}
            
        self.exec = {
            'upload': self.upload,
            'download': self.download,
            'read': self.read,
            'delete': self.delete,
            'shutdown': self.shutdown,
        }
    
    def check_heartbeats(self):
        """
        Check if a heartbeat has been received from each DataServer within the specified timeout.
        If not, print an error message.
        """
        while self.is_running:
            for ds in self.dataservers:
                current_time = time.time()
                if current_time - ds.last_heartbeat_time > self.heartbeat_timeout:
                    print(f"Error: No heartbeat received from DataServer {ds.identifier} for {self.heartbeat_timeout} seconds.")

            time.sleep(1)
        
    def delete(self, file_path):
        """
        Usage: delete [file_path]
        Delete a file from MiniDFS.
        """
        if not self.exists(file_path):
            print(f'{file_path} does not exist in MiniDFS.')
            return

        # Extract file name from file_path
        file_name = file_path.split('/')[-1]

        # Notify the user
        print(f'Deleting {file_path} from MiniDFS...')

        # Delete the file from the file tree
        dirs = [d for d in file_path.split('/') if d != '']
        last_dir = '/'.join(dirs[:-1])
        del self.to_dir(last_dir)['.files'][file_name]

        # Notify the user
        print(f'Deleted file entry from file tree.')

        # Instruct DataServers to delete file chunks
        for ds in self.dataservers:
            ds.cmd_chan.put(' '.join(['delete_chunks', file_path]))

        # Notify the user
        print(f'Deleted file chunks from DataServers.')


        
    def shutdown(self):
        self.is_running = False
        with open(self.file_tree_path, 'wb') as f:
            pickle.dump(self.file_tree, f)
        
            
    def upload(self, file: str, dir: str):
        curr = self.to_dir(dir)
        curr['.files'].add(file)
        
        data_servers = self.dataservers
        random.shuffle(data_servers)

        # Choose 3 random data servers to upload to.
        selected_servers = data_servers[:3]

        # send cmd to dataserver threads
        for ds in selected_servers:
            ds.cmd_chan.put(' '.join(['save_recv_chunks', os.path.join(dir, file)]))
            
        while True:
            (chunk, i) = self.in_chan.get()   
            # send to dataservers
            for ds in selected_servers:
                ds.in_chan.put((chunk, i))
            if not chunk:
                break
            
    def download(self, file_path):
        assert self.exists(file_path)

        # Start with the first data server.
        ds_index = 0

        # While we haven't downloaded the entire file, keep trying data servers.
        while True:
            # Get the current data server.
            ds = self.dataservers[ds_index]

            # Send the command to send the file chunks to the data server.
            ds.cmd_chan.put(' '.join(['output_file_chunks', file_path]))

            # Receive the file chunks from the data server.
            while True:
                # Get the next chunk from the data server.
                chunk = ds.out_chan.get()

                # Send the chunk to the client.
                self.out_chan.put(chunk)
                
                # If the chunk is empty, we're done.
                if not chunk:
                    return

            # Otherwise, try the next data server.
            ds_index = (ds_index + 1) % len(self.dataservers)

        # If we reach this point, we couldn't find the file on any of the data servers.
        raise FileNotFoundError(f"File not found: {file_path}")

    
    def read(self, file_path, loc, offset):
        assert self.exists(file_path)

        # Start with the first data server.
        ds_index = 0

        # While we haven't read the entire file, keep trying data servers.
        while True:
            # Get the current data server.
            ds = self.dataservers[ds_index]

            # Send the command to read the file chunks from the data server.
            ds.cmd_chan.put(' '.join(['read_file', file_path, loc, offset]))

            # Receive the file chunks from the data server.
            while True:
                # Get the next chunk from the data server.
                chunk = ds.out_chan.get()

                # Send the chunk to the client.
                self.out_chan.put(chunk)

                # If the chunk is empty, we're done.
                if not chunk:
                    return

            # Otherwise, try the next data server.
            ds_index = (ds_index + 1) % len(self.dataservers)

        # If we reach this point, we couldn't find the file on any of the data servers.
        raise FileNotFoundError(f"File not found: {file_path}")

        
    def mkdir(self, dir):
        curr_dir = self.file_tree
        dirs = [d for d in dir.split('/') if d != '']
        for d in dirs:
            if d not in curr_dir.keys():
                curr_dir[d] = {'.files': set()}
            curr_dir = curr_dir[d]
            
    def deldir(self, dir):
        dirs = [d for d in dir.split('/') if d != '']
        last_dir = '/'.join(dirs[:-1])
        del self.to_dir(last_dir)[dirs[-1]]
        
    def to_dir(self, dir):
        curr_dir = self.file_tree
        dirs = [d for d in dir.split('/') if d != '']
        for d in dirs:
            curr_dir = curr_dir[d]
        return curr_dir

    def ls(self, dir=''):
        curr = self.to_dir(dir)
        for k in curr.keys():
            if k == '.files':
                # file
                for f in curr[k]:
                    # Extract only the file name from the path
                    file_name = os.path.basename(f)
                    print(file_name, end=' ' * 2)
            else:
                # folder
                print(Fore.BLUE + k + Fore.RESET, end=' ' * 2)
        print()

        
    def exists(self, file_path):
        dirs = [d for d in file_path.split('/') if d != '']
        dir = '/'.join(dirs[:-1])
        file = dirs[-1]
        curr_dir = self.to_dir(dir)
        return file in curr_dir['.files']
    