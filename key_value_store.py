# KeyValueStore implementation
import os
import json
import threading
import collections
import logging
import hashlib

class KeyValueStore:
    def __init__(self, ip, port, cache_size, strategy, folder_path, is_prime):
        self.is_prime = is_prime
        self.ip = ip
        self.port = port
        self.number_of_files = 16 # number of files
        self.locks = [threading.Lock() for _ in range(self.number_of_files)] # lock for each file
        self.folder_path = folder_path # folder which data will be stored

        # Check if the /data folder exists
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        # Iterate over the file range from 0 to 15
        for i in range(self.number_of_files):
            file_name = str(i) + ".txt"
            data_file_path = os.path.join(self.folder_path, file_name)
            # Check if the file exists
            if not os.path.isfile(data_file_path):
                # Create the file if it doesn't exist
                with open(data_file_path, 'w') as file:
                    pass
                logging.debug(f"File {file_name} was missing and has been created.")
            else:
                logging.debug(f"File {file_name} exists.")

        self.cache_size = cache_size
        self.strategy = strategy
        self.cache = {}
        self.index = {i: {} for i in range(self.number_of_files)} # indexing for faster access to key in a file
        if strategy == 'FIFO':
            self.cache_info = collections.deque()
        elif strategy == 'LRU':
            self.cache_info = collections.OrderedDict()
        elif strategy == 'LFU':
            self.cache_info = collections.Counter()

        logging.debug("Creating file indexes!")
        for i in range(self.number_of_files):
            self._build_index(i)

        #self.print_cache()

    def compute_md5_hash(self, string):
        md5_hash = hashlib.md5()
        md5_hash.update(string.encode('utf-8'))
        return md5_hash.hexdigest()
    
    def _get_key_index(self, key):
        key = self.compute_md5_hash(key)
        return int(key[0], 16) % len(self.locks)

    def _get_file_index(self, hash_value):
        return int(hash_value[0], 16) % self.number_of_files

    def _get_lock(self, key):
        return self.locks[self._get_key_index(key)]

    def _get_file_path(self, key):
        return os.path.join(self.folder_path, f"{self._get_key_index(key)}.txt")

    def _get_file_path_from_index(self, i):
        return os.path.join(self.folder_path, f"{i}.txt")
    
    def _build_index(self, i):
        file_path = os.path.join(self.folder_path, f"{i}.txt")
        logging.debug(f"building index of: {file_path}")
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                pos = f.tell()
                line = f.readline()
                while line:
                    data = json.loads(line)
                    self.index[i][data['key']] = pos
                    pos = f.tell()
                    line = f.readline()
                    
    def print_cache(self):
        logging.debug(f"Cache: {self.cache}")
        logging.debug(f"Cache_info: {self.cache_info}")

    def evict(self):
        logging.debug("Cache is Full! Deleting element!")
        if self.strategy == 'FIFO':
            evicted_key = self.cache_info.popleft()
        elif self.strategy == 'LRU':
            evicted_key = next(iter(self.cache_info))
            del self.cache_info[evicted_key]
        elif self.strategy == 'LFU': # TODO check correctness
            evicted_key = self.cache_info.most_common()[:-2:-1][0][0]
            self.cache_info[evicted_key] -= 1
            if self.cache_info[evicted_key] <= 0:
                del self.cache_info[evicted_key]
        del self.cache[evicted_key]

    def get(self, key):
        # if self.is_prime is False:
        #     try:
        #         self.log_text += f"get-key:{key}\n"
        #         all_non_prime_data = self.get_all_data()
        #         non_prime_value = all_non_prime_data[key]
        #         self.log_text += f"get-key-value:{non_prime_value}\n"
        #         return f"get_success {key} {non_prime_value}"
        #     except Exception as e:
        #         raise Exception("Not Found in Non-Prime KVStore")
        with self._get_lock(key):
            response = f"get_success {key} "
            logging.debug(f"Getting key:{key}")
            self.print_cache()
            # check whether key exists in cache
            if key in self.cache:
                logging.debug("key exists in cache!")
                if self.strategy == 'LRU':
                    self.cache_info.move_to_end(key)
                elif self.strategy == 'LFU':
                    self.cache_info[key] += 1
                self.print_cache()
                return response + self.cache[key]

            logging.debug("key does not exists in cache! Getting from file.")
            # cache does not contain current key
            file_index = self._get_key_index(key) # get which file contains key
            logging.debug(f"file_index of cur key: {file_index}")
            # get lock to read file (waits if someone else has the lock)
            if key in self.index[file_index]:
                with open(self._get_file_path(key), 'r') as f:
                    f.seek(self.index[file_index][key])
                    value = f.readline()
                    logging.debug(f"read from file:{value}")
                    data = json.loads(value)
                    logging.debug(f"value:{data['value']}")

                    # update cache
                    if len(self.cache) >= self.cache_size:
                        self.evict()
                    self.cache[key] = data['value']
                    if self.strategy == 'FIFO':
                        self.cache_info.append(key)
                    elif self.strategy == 'LRU':
                        self.cache_info[key] = None
                        self.cache_info.move_to_end(key)
                    elif self.strategy == 'LFU':
                        self.cache_info[key] += 1
                    self.print_cache()
                    return response + data['value']
            else:
                raise Exception('Key does not exists!')
    def find_value_in_file(self, key):
        value = None
        with open(self._get_file_path(key), 'r') as read_f:
            pos = read_f.tell()
            line = read_f.readline()
            while line:
                data = json.loads(line)  # load data from file

                if str(data['key']) == str(key):

                    if self.is_prime is False:
                        self.log_text += f"deger geldi: {data['value']}\n"

                    value = data['value']

                pos = read_f.tell()
                line = read_f.readline()
        return value

    def put(self, key, value):
        with self._get_lock(key):
            response = "put_"
            file_index = self._get_key_index(key)
            # check is this update or set new key operation
            if key in self.index[file_index]:
                response += f"update {key}"
            else:
                response += f"success {key}"

            logging.debug("Setting key!")
            logging.debug(f"key:{key}")
            logging.debug(f"value:{value}")
            self.print_cache()
            if len(self.cache) >= self.cache_size:
                self.evict()
            self.cache[key] = value
            if self.strategy == 'FIFO':
                self.cache_info.append(key)
            elif self.strategy == 'LRU':
                self.cache_info[key] = None
                self.cache_info.move_to_end(key)
            elif self.strategy == 'LFU':
                self.cache_info[key] += 1
            self.print_cache()
            self._save_to_disk(key, value)
            return response

    def delete(self, key):
        with self._get_lock(key):
            logging.debug(f"Deleting key: {key}")
            self.print_cache()
            file_index = self._get_key_index(key)
            if key in self.cache:
                del self.cache[key]
                if self.strategy == 'FIFO':
                    self.cache_info.remove(key)
                elif self.strategy == 'LRU':
                    self.cache_info.pop(key)
                elif self.strategy == 'LFU':
                    del self.cache_info[key]
                    # self.cache_info[key] -= 1
                logging.debug("deleted from cache")
                self.print_cache()
            if key in self.index[file_index]:
                logging.debug("Deleting from file")
                value = self._delete_from_disk(key)
                return f"delete_success {key} {value}"
            else:
                raise Exception('Key does not exists!')

    def _save_to_disk(self, key, value):
        file_path = self._get_file_path(key)
        file_index = self._get_key_index(key)
        logging.debug(f"file_path:{file_path}")
        logging.debug(f"file_index:{file_index}")
        with open(file_path, 'a+') as f:
            # Save the current position which is at the start of the new line
            self.index[file_index][key] = f.tell()
            data = {'key': key, 'value': value}
            f.write(json.dumps(data) + '\n')

    def _delete_from_disk(self, key):
        file_path = self._get_file_path(key)
        temp_file_path = file_path + '.tmp'
        logging.debug(f"file_path:{file_path}")
        logging.debug(f"temp_file_path:{temp_file_path}")
        value = ""
        with open(file_path, 'r') as read_f, open(temp_file_path, 'w') as write_f:
            pos = read_f.tell()
            line = read_f.readline()
            while line:
                data = json.loads(line)
                if data['key'] != key:
                    write_f.write(line)
                else:
                    value = data['value']
                pos = read_f.tell()
                line = read_f.readline()
        os.remove(file_path)
        os.rename(temp_file_path, file_path)
        self._build_index(self._get_key_index(key))
        return value

    def hexToInt(self, hex_string):
        return int(hex_string, 16)
    
    def get_data_to_transfer(self, start, end): # prepare data transfer it to a new server
        sendData = {} # data to send to new server
        start_file_index = self._get_file_index(start) # get file index
        end_file_index = self._get_file_index(end) # get file index
        if self.hexToInt(start) < self.hexToInt(end):
            for i in range(start_file_index, end_file_index + 1): # get data from files
                file_path = self._get_file_path_from_index(i) # get file path
                temp_file_path = file_path + '.tmp' # created temp file path
                logging.debug(f"file_path:{file_path}")
                logging.debug(f"temp_file_path:{temp_file_path}")
                # value = ""
                with open(file_path, 'r') as read_f, open(temp_file_path, 'w') as write_f:
                    pos = read_f.tell()
                    line = read_f.readline()
                    while line:
                        data = json.loads(line) # load data from file
                        hashed_data = self.compute_md5_hash(data['key']) # compute hash of key
                        if self.hexToInt(hashed_data) >= self.hexToInt(start) and self.hexToInt(hashed_data) <= self.hexToInt(end): # check if key is in range
                            sendData[data['key']] = data['value'] # add data to send
                        else:
                            write_f.write(line) # write data to temp file if not in range
                        pos = read_f.tell()
                        line = read_f.readline()                    
                # os.remove(file_path)
                # os.rename(temp_file_path, file_path)
                # self._build_index(self._get_key_index(key))
                # return value
            return sendData
        else:            
            for i in range(start_file_index, end_file_index + self.number_of_files + 1): # traverse files e.g 14,15,0,1,2
                file_path = self._get_file_path_from_index(i%self.number_of_files) # get file path
                temp_file_path = file_path + '.tmp' # created temp file path
                logging.debug(f"file_path:{file_path}")
                logging.debug(f"temp_file_path:{temp_file_path}")
                # value = ""
                with open(file_path, 'r') as read_f, open(temp_file_path, 'w') as write_f:
                    pos = read_f.tell()
                    line = read_f.readline()
                    while line:
                        data = json.loads(line) # load data from file
                        hashed_data = self.compute_md5_hash(data['key']) # compute hash of key
                        if self.hexToInt(hashed_data) >= self.hexToInt(start) or self.hexToInt(hashed_data) <= self.hexToInt(end): # check if key is in range
                            sendData[data['key']] = data['value'] # add data to send
                        else: 
                            write_f.write(line) # write data to temp file if not in range
                        pos = read_f.tell()
                        line = read_f.readline()    
            return sendData
        

    def is_ip_port_mine(self, ip, port):
        if str(self.ip) == str(ip) and str(self.port) == str(port):
            return True

    def get_all_data(self): # prepare data transfer it to a new server
        sendData = {} # data to send to new server
        for i in range(self.number_of_files): # traverse all files
            file_path = self._get_file_path_from_index(i) # get file path
            with open(file_path, 'r') as read_f:
                pos = read_f.tell()
                line = read_f.readline()
                while line:
                    data = json.loads(line) # load data from file
                    sendData[data['key']] = data['value'] # add all data to send
                    pos = read_f.tell()
                    line = read_f.readline()    
        return sendData

    def is_responsible(self, key, metadata): # check if server is responsible for a key
        # find my start and end inside metadata array of dictionaries using self.my_ip and self.my_port
        org_key = key
        start = None
        end = None
        for i in range(len(metadata)):
            if str(metadata[i]['ip']) == str(self.ip) and str(metadata[i]['port']) == str(self.port):
                start = metadata[i]['start']
                end = metadata[i]['end']
                break
        key = self.compute_md5_hash(key)
        if self.hexToInt(start) <= self.hexToInt(key) <= self.hexToInt(end) or (self.hexToInt(start) > self.hexToInt(end) and (self.hexToInt(key) >= self.hexToInt(start) or self.hexToInt(key) <= self.hexToInt(end))): # right side of or for the first node in the ring
            print("I am responsible for this key " + str(org_key) + " " + str(self.ip) + " " + str(self.port))
            return True
        else:
            print("I am not responsible for this key " + str(org_key) + " " + str(self.ip) + " " + str(self.port))            
            return False


    def putTransferredData(self, data): # receive data from other server
        # print("putTransferredData")
        # print(data)
        if data is None or data == "{}":
            return
        for key, value in data.items(): # iterate through data
            with self._get_lock(key): 
                self._save_to_disk(key, value) # save data to disk
        #logging.error("Data saved to disk")

    def deleteTransferredData(self):
        # Get a list of all files in the folder
        file_list = os.listdir(self.folder_path)
        #logging.error("deleting tmp files started")
        for file_name in file_list:
            # Check if the file ends with .txt.tmp
            if file_name.endswith(".txt.tmp"):
                # Extract the base file name without the extension
                base_name = file_name.split(".")[0] # get file name without extension
                with self.locks[int(base_name)]:
                    # Create the new file name by removing the ".tmp" extension
                    new_name = os.path.join(self.folder_path, base_name + ".txt")
                    
                    # Delete any existing .txt file with the same name
                    if os.path.exists(new_name):
                        os.remove(new_name)
                    
                    # Rename the .txt.tmp file to .txt
                    file_path = os.path.join(self.folder_path, file_name)
                    os.rename(file_path, new_name)
                    
                    self._build_index(int(base_name)) # calculate indexes of keys in file
                    logging.debug(f"Renamed {file_name} to {base_name}.txt")
        #logging.error("deleting tmp files finished")


