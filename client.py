import os
import shutil
import tempfile
import traceback
import hashlib

from servers import NameServer, DataServer

CHUNK_SIZE = 2 * 1024 * 1024

class Client:
    def __init__(self):
        # minidfs state
        self.is_running = True
        
        # minidfs location
        self.dfs_path = './dfs'
        os.makedirs(self.dfs_path, exist_ok=True)
                
        # minidfs settings
        self.num_dataserver = 4
        self.dataservers = [
            DataServer(
                root_path=os.path.join(self.dfs_path, 'dataserver' + str(i))
            ) for i in range(self.num_dataserver)
        ]
        self.nameserver = NameServer(
            root_path=os.path.join(self.dfs_path, 'nameserver'),
            dataservers=self.dataservers
        )
        
        # start server threads
        for i in range(self.num_dataserver):
            self.dataservers[i].start()
        self.nameserver.start()
        
        # commands exec dict
        self.exec = {
            # basic shell operations
            'help': self.cmd_help,
            'exit': self.exit_minidfs,
            
            # dfs file structure
            'mkdir': self.mkdir,
            'deldir': self.deldir,
            'exists': self.exists,
            'ls': self.ls,
            'tree': self.tree,
            
            # read/upload file interface
            'upload': self.upload,
            'download': self.download,
            'read': self.read,
            
            # recovery
            # 'delds': self.delds,
            'recover': self.recover,
            'check_md5sum': self.check_md5sum,
            #'move': self.move,
        }
        
        print('Support operations:', '|'.join(self.exec.keys()))
        
    def cmd_help(self, c=None):
        """
        Usage: help [command]
        To show the help information of command
        """
        def prettify(s):
            return '\n'.join([line.lstrip() for line in s.split('\n') if line])
        
        if c is None:
            print(prettify(self.cmd_help.__doc__))
        else:
            print(prettify(self.exec[c].__doc__))
        
    def exit_minidfs(self):
        """
        Usage: exit
        Exit MiniDFS client and shutdown all servers.
        """
        self.is_running = False
        self.nameserver.cmd_chan.put('shutdown')
        for i in range(self.num_dataserver):
            self.dataservers[i].cmd_chan.put('shutdown')
        
    def mkdir(self, dir):
        """
        Usage: mkdir [dir]
        make a directory.
        """
        self.nameserver.mkdir(dir)
        
    def deldir(self, dir):
        """
        Usage: del [dir]
        delete a directory.
        """
        self.nameserver.deldir(dir)
        
    def ls(self, dir=''):
        """
        Usage: ls [dir]
        list files and folders in a directory.
        """
        self.nameserver.ls(dir)
        
    def tree(self):
        """
        Usage: tree
        print the whole directory tree of MiniDFS.
        """
        print(self.nameserver.file_tree)
        
    def exists(self, file_path):
        """
        Usage: exists folder/.../file
        check whether a *file* is existing in MiniDFS.
        """
        ret = self.nameserver.exists(file_path)
        print(ret)
        
    def upload(self, src_file, des_dir=''):
        """
        Usage: upload [src_file] [des_dir]
        upload local file to MiniDFS destination.
        """
        if not os.path.isfile(src_file):
            print('{} is not a file.'.format(src_file))
            return
        
        # send cmd to nameserver thread
        self.nameserver.cmd_chan.put(' '.join(['upload', src_file, des_dir]))
        
        # send data into channel
        with open(src_file, 'rb') as f:            
            i = 0
            while True:
                chunk = f.read(CHUNK_SIZE)
                self.nameserver.in_chan.put((chunk, i))
                i += 1
                if not chunk:
                    break
    
    def download(self, file_path, save_dir):
        """
        Usage: download [file_path] [save_dir]
        download file from MiniDFS and save it to local directory.
        """
        if not self.nameserver.exists(file_path):
            print('{} does not exist in MiniDFS.'.format(file_path))
            return 
        
        os.makedirs(save_dir, exist_ok=True)
        
        # send cmd to nameserver thread
        self.nameserver.cmd_chan.put(' '.join(['download', file_path]))
        with open(os.path.join(save_dir, file_path.split('/')[-1]), 'wb') as f:
            while True:
                chunk = self.nameserver.out_chan.get() 
                if not chunk:
                    break
                f.write(chunk)
                    
    def read(self, file_path, loc=0, offset=10):
        """
        Usage: read [file] [loc] [offset]
        read a file from `loc` to `loc + offset`
        """
        if not self.nameserver.exists(file_path):
            print('{} does not exist in MiniDFS.'.format(file_path))
            return 
        
        # send cmd to nameserver thread
        self.nameserver.cmd_chan.put(' '.join(['read', file_path, loc, offset]))
        while True:
            chunk = self.nameserver.out_chan.get() 
            if not chunk:
                break
            print(chunk, end='')
        print()
        
    # def delds(self, ds_id):
    #     """
    #     Usage: delds [ds_id]
    #     delete the dataserver whose id is `ds_id`
    #     """
    #     ds = self.dataservers[int(ds_id)]
    #     ds.save_state()
    #     shutil.rmtree(ds.root_path)
        
    def recover(self, ds_id):
        """
        Usage: recover [ds_id]
        recover the dataserver whose id is `ds_id` with the help of other normal dataservers.
        """
        ds = self.dataservers[int(ds_id)]
        normal_id = [i for i in range(self.num_dataserver) if i != int(ds_id)][0]
        normal_ds = self.dataservers[normal_id]
        normal_ds.save_state()
        shutil.copytree(normal_ds.root_path, 
                        ds.root_path, dirs_exist_ok=True)
        
    def _check_md5sum(self):
        # check same file names
        ds_files = []
        for ds in self.dataservers:
            files = os.listdir(ds.root_path)
            files.sort()
            ds_files.append(files)
        same_files = all(files == ds_files[0] for files in ds_files)
        if not same_files:
            print('diff names')
            return False
        
        # check same md5
        def md5(file):
            with open(file, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        
        ds_md5s = []
        for file in files:
            md5s = [md5(os.path.join(ds.root_path, file)) for ds in self.dataservers]
        md5s.sort()
        ds_md5s.append(md5s)
        same_md5s = all(md5s == ds_md5s[0] for md5s in ds_md5s)
        if not same_md5s:
            print('diff md5')
            # for i in range(self.num_dataserver):
            #     print(ds_md5s[i])
            return False
        
        return True
    
    def check_md5sum(self):
        """
        Usage: check_md5sum
        check whether there are same contents in different dataservers.
        """
        print(self._check_md5sum())

    def check_server_status(self):
        # Check the status of servers and take appropriate actions
        for server in [self.nameserver] + self.dataservers:
            if not server.is_alive:
                print(f"Server {server.__class__.__name__} is not responding.")   
        
    def run(self):
        while self.is_running:
            argv = input('[MiniDFS]$ ').split()
            if len(argv):
                cmd = argv[0]
                args = argv[1:]
                try:
                    self.exec[cmd](*args)
                except Exception:
                    traceback.print_exc()
        self.check_server_status()
    
    def move(self, src_path, dest_path):
        """
        Usage: move [src_path] [dest_path]
        Move a file from one directory to another.
        """
        try:
            # Create a temporary directory
            # temp_download_dir = tempfile.mkdtemp()
            temp_download_path = os.path.join("./temp_download_dir", src_path.split('/')[-1])

            # Download the file to the temporary directory
            self.download(src_path, "./temp_download_dir")

            # Upload the file to the new location
            self.upload(temp_download_path, dest_path)

            # Clean up: delete the file from the temporary directory
            # shutil.rmtree("./temp_download_dir")
            print(f"{src_path} moved to {dest_path}.")
        except Exception as e:
            print(f"Error moving {src_path} to {dest_path}: {e}")

        # # Delete the file from the source
        # self.nameserver.cmd_chan.put(f'delete_chunks {src_path}')
        # self.nameserver.deldir(src_path)  # Assuming the source path is a file, not a directory
            