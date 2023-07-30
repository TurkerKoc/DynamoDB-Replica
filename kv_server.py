import argparse
import logging
import socketserver
import os
import key_value_store
import hashlib
import json
import socket
import signal
import threading
import sys 
import shutil
import copy
import asyncio
import websockets
import random
import queue
import time


metadata = []
server_stopped = True
server_write_lock = False
server_info = {}
kvs = []
isShuttingDown = False #TODO change this to true when shutdown hook is called
bootstrap = None
server = None
thread = None

# Dictionary to store subscribed clients for each channel
channels = {'key': {}, 'operation': {}}
eventQueue = queue.Queue() # queue for storing messages to be published

async def subscribe(websocket, subscription_type, name):
    print(f"subscribing to {subscription_type}: {name}")
    if name not in channels[subscription_type]:
        channels[subscription_type][name] = set()
    channels[subscription_type][name].add(websocket)
    print(channels)

async def unsubscribe(websocket, subscription_type, name):
    print("inside unsbs")
    if name in channels[subscription_type]:
        channels[subscription_type][name].discard(websocket)
    print(channels)

async def handle_client(websocket, path):
    # Assume clients will send a JSON message to subscribe/unsubscribe
    async for message in websocket:
        data = json.loads(message)
        print(f"handle {data}")
        if data.get('action') == 'subscribe':
            await subscribe(websocket, data.get('type'), data.get('name'))
        elif data.get('action') == 'unsubscribe':
            await unsubscribe(websocket, data.get('type'), data.get('name'))

def run_async_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start_server = websockets.serve(handle_client, '0.0.0.0', 8765)
    loop.run_until_complete(start_server)
    loop.run_until_complete(publish_data())
    loop.run_forever()


async def publish_data():
    # eventQueue.put(('get', 'test', 'test'))
    while True:
        print("at the beginning")
        await asyncio.sleep(1)  # Publish data every 1 second
        print("after wait")
        if eventQueue.empty():
            continue

        print("publishing data")
        operation, key, value = eventQueue.get()        
        # Simulate some data to be published to clients
        data = {
            'operation': operation,
            'key': key,
            'value': value
        }
        print(data)
        print(channels)
        
        # if channels.get('operations', {}).get(operation) or channels.get('keys', {}).get(key):
            # There's at least one websocket to send data to

        data['type'] = "operation"
        # Send data to clients subscribed to this operation
        for websocket in channels['operation'].get(operation, set()):
            
            print("sending data")
            try:
                await websocket.send(json.dumps(data))
            except:
                pass

        data['type'] = "key"
        # Send data to clients subscribed to this key
        for websocket in channels['key'].get(key, set()):
            print("sending data")
            try:
                await websocket.send(json.dumps(data))
            except:
                pass

        eventQueue.task_done()

        # for websocket in channels.get(channel, set()):
        #     print("sending data")
        #     await websocket.send(json.dumps(data))
        # eventQueue.task_done()

def configure_logging(file_path, level=logging.INFO):
    # if not os.path.exists(os.path.join(os.getcwd(),file_path)):
    #     open(os.path.join(os.getcwd(),file_path), 'w').close()
    if args.loglevel in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        pass
    else:
        print("Invalid log level. Use DEBUG, INFO, WARNING, ERROR or CRITICAL.")
        exit(1)
    log_dir = os.path.dirname(file_path)
    os.makedirs(os.path.join(os.getcwd(), log_dir), exist_ok=True)
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(file_path),  # Specify the file name here
            # logging.StreamHandler()  # Optionally, log messages can also be displayed in the console
        ]
    )

def formatMetaData(): #prepare metadata to send to client
    global metadata
    response = "keyrange_success "
    for dict in metadata:
        start = dict['start']
        end = dict['end']
        ip = dict['ip']
        port = dict['port']
        response += f"{start},{end},{ip}:{port};"
    # add carriage return at the end
    response += "\r\n"
    return response


def getMetaDataRead(): #prepare metadata to send to client
    global metadata
    metadata_read = []
    if len(metadata) >= 3:
        for i in range(len(metadata)):
            metadata_read.append(copy.deepcopy(metadata[i])) # deep copy
            metadata_read[i]['start'] = metadata[i-2]['start']
    else:
        metadata_read = copy.deepcopy(metadata) # deep copy
    return metadata_read


def formatMetaDataRead(): #prepare metadata to send to client
    metadata_read = getMetaDataRead()
    response = "keyrange_read_success "

    for dict in metadata_read:
        start = dict['start']
        end = dict['end']
        ip = dict['ip']
        port = dict['port']
        response += f"{start},{end},{ip}:{port};"
    # add carriage return at the end
    response += "\r\n"
    return response

def transferDataToServer(ip, port, start, end): # transfer data to the server
    data = kvs[0].get_data_to_transfer(start, end) # get data to transfer
    data = "data " + json.dumps(data) + "\r\n" # convert to json string
    responseStatus = False
    # connect to the server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, int(port)))
        ignoreWelcomeMessage(s) # ignore welcome message
        s.sendall(data.encode('utf-8'))

        if isShuttingDown:
            response = s.recv(1024)
            response = response.decode('ascii') # decode response
            if(response == "data ok"): # other server received your data and acknowledged it
                responseStatus = True
        else:
            responseStatus = True
        s.close()
    return responseStatus

def operationsAfterTransferDone():
    # print("Operations after transfer called")
    #TODO delete old data and make tmp file as new data
    kvs[0].deleteTransferredData()
    return ""

def updateMetaData(keyrange): # update metadata
    global metadata
    # print("before update")
    # print(metadata)
    metadata = keyrange
    # print("after update")
    # print(metadata)

def compute_md5_hash(string):
    md5_hash = hashlib.md5()
    md5_hash.update(string.encode('utf-8'))
    return md5_hash.hexdigest()

def hexToInt(hex_string):
    return int(hex_string, 16)

def isServerResponsible(key): # check if server is responsible for the key range
    global metadata
    global server_info
    # print(f"key {key} server_info: {server_info} metadata: {metadata}")
    # print(metadata)
    # print("server_info: ")
    # print(server_info)
    for dict in metadata:
        if dict['ip'] == server_info['ip'] and int(dict['port']) == int(server_info['port']): # if current element is this server's ip and port
            # hash key and check if it is in the range        
            key = compute_md5_hash(key)
            if hexToInt(dict['start']) <= hexToInt(key) <= hexToInt(dict['end']) or (hexToInt(dict['start']) > hexToInt(dict['end']) and (hexToInt(key) >= hexToInt(dict['start']) or hexToInt(key) <= hexToInt(dict['end']))): # right side of or for the first node in the ring
                return True
            else:
                return False
    return False

def transferDoneToEcs(): # send transfer done message to ecs
    #TODO send message to ecs
    global client_socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ecs_ip=bootstrap.split(':')[0]
    ecs_port=bootstrap.split(':')[1]
    try:
        # print(ecs_ip,ecs_port)
        client_socket.connect((ecs_ip, int(ecs_port)))
        # logging.info("Connected to server, waiting for reply...")
        # print("Sending data recieved to ecs")
        message= f'received_data'
        client_socket.sendall(message.encode("ascii"))

    except Exception as e:
        logging.error(f"Failed to connect to server: {e}")
        client_socket = None
    return ""

def CommandHandler(command):
    # print("")
    # print(f"command:{command}")
    # print("")
    global metadata
    global server_stopped
    global server_write_lock
    # global gettingData
    global server_info
    command = command.rstrip() # remove trailing whitespace and newline
    operation = command.split(' ')[0]
    # print(f"-- Operation {operation} ")
    if operation in ('put', 'get', 'delete', 'keyrange', 'keyrange_read'):
        logging.info(f"'{operation}' operation called")
        if operation in ('put', 'get', 'delete') and server_stopped: #if server is in initialization and not ready to serve
            return 'server_stopped'
        elif operation == 'put':
            try:
                print("command: " + command)
                key = command.split(' ')[1]
                value = ' '.join(command.split(' ')[2:])
                if isServerResponsible(key):
                    print("server is reponsible == replicating command:" + command)
                    trigger_replica_operation(command)
                    eventQueue.put(('put',key,value)) #put key value in queue
                    return kvs[0].put(key, value)                    
                else:
                    return 'server_not_responsible'
            except Exception as e:
                print(e)
                return 'put_error'
        elif operation == 'get':
            print("command: " + command)
            try:
                key = command.split(' ')[1]
                print(f"key: {key}")
                for curr_kv in kvs:
                    if curr_kv.is_responsible(key, metadata):
                        eventQueue.put(('get',key, curr_kv.get(key))) #get operation to queue
                        return curr_kv.get(key)

                return 'server_not_responsible'
            except Exception as e:
                key = command.split(' ')[1]
                print(e)
                return 'get_error ' + key
        elif operation == 'delete':
            try:
                key = command.split(' ')[1]    
                if isServerResponsible(key):                    
                    trigger_replica_operation(command)
                    eventQueue.put(('delete',key,None)) #delete operation to queue
                    return kvs[0].delete(key)
                else:
                    return 'server_not_responsible'
            except Exception as e:
                key = command.split(' ')[1]
                print(e)
                return 'delete_error ' + key
        elif operation == 'keyrange':
            try:                
                return formatMetaData()
            except Exception as e:
                print(e)
                return 'keyrange_error'
        elif operation == 'keyrange_read':
            try:
                return formatMetaDataRead()
            except Exception as e:
                print(e)
                return 'keyrange_read_error' 

    elif operation == 'data': # if other server is sending data to you
        try:
            # gettingData = True

            data = ''.join(command.split(' ')[1:])
            data = json.loads(data.replace("'",'"')) # convert json string to python dictionaryuuuuuuuuuuuuuuuuuuu
            kvs[0].putTransferredData(data) # put data in the kvs
            # print('i have recieved data from another server',metadata)
            if server_stopped: # if server is in initialization
                transferDoneToEcs() # send transfer done message to ecs
                return "close_connection"
            else:
                return 'data_ok'
        except Exception as e:
            print(e)
            return 'data_error'
    elif operation == 'data_ok': # if other server received your data and acknowledged it
        transferDoneToEcs() # send transfer done message to ecs
        return "close_connection"
    elif operation  == 'send_data':
        # print('recieved data send command')
        command_args=command.split(' ')
        if server_write_lock == False: # get into transferring data mode
            server_write_lock = True
        transferDataToServer(command_args[1], command_args[2], command_args[3], command_args[4])
    elif operation == 'metadata_broadcast':
        if server_stopped:
            server_stopped = False
        # global metadata
        keyrange=command.split('  ')[1].split(' \r')[0] #first split is with two spaces
        keyrange = json.loads(keyrange.replace("'",'"'))
        
        if server_write_lock == True: # delete temp files for the server who sends data
            operationsAfterTransferDone()
            server_write_lock = False

        # print("new keyrange received and handle MetaData Change called")
        # HANDLE METADATA CHANGE FOR REPLICATION
        handleMetaDataChange(keyrange)
        updateMetaData(keyrange)
        # gettingData = False
        # return "broadcast_recieved"
        return "close_connection"
    elif operation == 'ping':
        return "pong"
    elif operation == 'replica':
        ip = command.split(' ')[1]
        port = command.split(' ')[2]
        
        command = ' '.join(command.split(' ')[3:])
        operation = command.split(' ')[0]

        for curr_kvs in kvs:
            if curr_kvs.is_ip_port_mine(ip, port):
                if operation == 'put':
                    key = command.split(' ')[1]
                    value = ' '.join(command.split(' ')[2:])
                    curr_kvs.put(key, value)
                elif operation == 'delete':
                    key = command.split(' ')[1]
                    curr_kvs.delete(key)
                else:
                    logging.error(f"Invalid operation '{operation}' requested")
                    return 'error unknown command!'
                break
        return "close_connection"
    elif operation == 'replica_transfer_data':
        ip = command.split(' ')[1]
        port = command.split(' ')[2]
        data = ''.join(command.split(' ')[3:])
        # print("recieved replica transfer data command")
        # print("data is ",data)
        exist = False
        data = json.loads(data.replace("'",'"')) # convert json string to python dictionaryyyyyyyyyyyyyyyy
        for curr_kvs in kvs:
            if curr_kvs.is_ip_port_mine(ip, port):
                # print("found replica kvs")
                exist = True
                curr_kvs.putTransferredData(data) # put data in the kvs
                break
        if exist is False:
            create_replica_kvs_and_put_data(ip, port,data)
            # returned_kvs.putTransferredData()
        return "close_connection"
    else:
        logging.error(f"Invalid operation '{operation}' requested")
        return 'error unknown command!'
    



def handleMetaDataChange(keyrange):
    # print("handleMetaDataChange started")
    global metadata
    
    # print("old metadata ",metadata)
    # print("new metadata ",keyrange)

    old_count = len(metadata)
    new_count = len(keyrange)
    # print("old count ",old_count)
    # print("new count ",new_count)

    old_my_index = next((i for i, dictionary in enumerate(metadata) if str(dictionary['ip']) == str(args.address) and str(dictionary['port']) == str(args.port)), -1)
    new_my_index = next((i for i, dictionary in enumerate(keyrange) if str(dictionary['ip']) == str(args.address) and str(dictionary['port']) == str(args.port)), -1)
    # print("old my index ",old_my_index)
    # print("new my index ",new_my_index)

    if new_my_index == -1:
        print('ERROR: my ip-port not found in old keyrange or new keyrange')
        return

    # has at least 3 servers -> replication is possible
    if new_count > 2:
        if(old_my_index == -1): # if i am a new server
            pass
            # print("i am a new server")


        # print("number of servers > 2")
                # AFTER
        new_one_after = keyrange[(new_my_index+1)%new_count]
        new_two_after = keyrange[(new_my_index+2)%new_count]

        # first replication started
        if old_count == 2 or old_count == 0: # if there was no replication before
            # print('send replica_transfer_data to 2 after kv_servers')
            trigger_replica_transfer_data(new_one_after)
            trigger_replica_transfer_data(new_two_after)
        # continued replication
        else:
            # OLD
                # BEFORE
            old_one_before = metadata[old_my_index-1]
            old_two_before = metadata[old_my_index-2]

                    # AFTER
            old_one_after = metadata[(old_my_index+1)%old_count]
            old_two_after = metadata[(old_my_index+2)%old_count]


            # NEW
                    # BEFORE
            new_one_before = keyrange[new_my_index-1]
            new_two_before = keyrange[new_my_index-2]
            # print("continued replication")
            # one before deleted
            if is_different_servers(old_one_before, new_one_before) and is_different_servers(old_one_before, new_two_before): 
                # print('merge data')
                merge_data_from_replica(old_one_before)

                # print('delete replica of old_one_before')
                remove_kvs_and_files(old_one_before)

                # print('send replica_transfer_data to 2 after kv_servers')
                trigger_replica_transfer_data(new_one_after)
                trigger_replica_transfer_data(new_two_after)
            # two before deleted
            if is_different_servers(old_two_before, new_two_before) and is_different_servers(old_two_before, new_one_before):
                # print('delete replica os old_two_before')
                remove_kvs_and_files(old_two_before)
            # new server added to one after
            if is_different_servers(new_one_after, old_one_after) and is_different_servers(new_one_after, old_two_after):
                # print('send replica_transfer_data to new_one_after')
                trigger_replica_transfer_data(new_one_after)
            # new server added to two after
            if is_different_servers(new_two_after, old_two_after) and is_different_servers(new_two_after, old_one_after):
                # print('send replica_transfer_data to new_two_after')
                trigger_replica_transfer_data(new_two_after)
    # has 2 servers -> replication is not possible
    elif new_count == 2 and old_count == 3:
        # print("number of servers = 2")
        old_one_before = metadata[old_my_index-1]
        # print("old_one_before ",old_one_before)

        new_one_before = keyrange[new_my_index-1]
        # print("new_one_before ",new_one_before)
        new_two_before = keyrange[new_my_index-2]
        # print("new_two_before ",new_two_before)

        # one before deleted
        if is_different_servers(old_one_before, new_one_before) and is_different_servers(old_one_before, new_two_before):
            # print('merge data with old_one_before')
            merge_data_from_replica(old_one_before)
        #delete all existing replicas
        # print('delete all replicas')
        # Remove all replicas
        remove_all_replicas()
        
def remove_all_replicas():
    # travers kvs and delete all replicas except my own data which is in kvs[0]
    kvs_copy = kvs.copy()
    for i in range(1, len(kvs)):
        remove_kvs_and_files(kvs_copy[i])

def is_different_servers(dict1, dict2):
    if str(dict1['ip']) == str(dict2['ip']) and str(dict1['port']) == str(dict2['port']):
        return False
    else:
        return True


def trigger_replica_transfer_data(dict_server_info):
    receiver_ip = dict_server_info["ip"]
    receiver_port = dict_server_info["port"]

    all_my_data = kvs[0].get_all_data()
    # start pinging server
    send_replica_thread = threading.Thread(target=send_replica_transfer_data, args=(receiver_ip, int(receiver_port), all_my_data))
    send_replica_thread.start()

def send_replica_transfer_data(ip, port, all_my_data): 
    all_my_data = "replica_transfer_data " + str(args.address) + " " + str(args.port) + " " + json.dumps(all_my_data) + "\r\n" # convert to json string
    # print("all my data")
    # print(all_my_data)
    # Open socket and transfer data to ip-port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, int(port)))
        ignoreWelcomeMessage(s) # ignore welcome message
        s.sendall(all_my_data.encode('ascii'))
        #s.close()

def trigger_replica_operation(command):    
    global metadata

    #check number of servers if <3 -> no replication possible
    if len(metadata) <= 2:
        return
    send_command = f"replica {args.address} {args.port} {command}\r\n" #replica ip port command

    #get 2 after servers
    my_index = next((i for i, dictionary in enumerate(metadata) if str(dictionary['ip']) == str(args.address) and str(dictionary['port']) == str(args.port)), -1)
    
    one_after = metadata[(my_index+1)%len(metadata)]
    two_after = metadata[(my_index+2)%len(metadata)]

    #send data to 2 after kv_servers
    send_replica_thread_one_after = threading.Thread(target=send_replica_operation, args=(one_after["ip"], int(one_after["port"]), send_command))
    send_replica_thread_one_after.start()
    send_replica_thread_two_after = threading.Thread(target=send_replica_operation, args=(two_after["ip"], int(two_after["port"]), send_command))
    send_replica_thread_two_after.start()

    # send_replica_operation(one_after["ip"], int(one_after["port"]), command)
    # send_replica_operation(two_after["ip"], int(two_after["port"]), command)

def send_replica_operation(ip, port, command):         
    # Open socket and transfer data to ip-port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, int(port)))
        ignoreWelcomeMessage(s) # ignore welcome message
        s.sendall(command.encode('utf-8'))
        s.close()

def HeartBeatCommandHandler(command):
    # print(f"command:{command}")
    global metadata
    global server_stopped
    global server_write_lock
    # global gettingData
    global server_info
    command = command.rstrip() # remove trailing whitespace and newline
    operation = command.split(' ')[0]
    # print(f"-- Operation {operation} server_info: {server_info} metadata: {metadata}, serverstopped: {server_stopped}, writelock: {server_write_lock}")
    if operation == 'ping':
        return "pong"
    else:
        logging.error(f"Invalid operation '{operation}' requested")
        return 'error unknown command!'

class MyHeartBeatRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # print(f"Connected to {self.client_address[0]}:{self.client_address[1]}")
        logging.info(f"Connected to {self.client_address[0]}:{self.client_address[1]}")
        logging.debug(f"this is just debug logs")

        welcome_message = "Welcome to the key value storage server!\r\n"
        self.request.sendall(welcome_message.encode())
        self.request.settimeout(20)
        while True:
            # Receive data from the client
            try:
                data = self.receive_large_data() # receive data from client (can be larger than 1024 bytes)
            except Exception as e:
                continue

            if not data:
                break

            resp = HeartBeatCommandHandler(data.decode('ascii'))

            # print(f"resp: {resp}")
            if resp == 'close_connection':
                break
            elif resp == 'data_ok':
                self.request.sendall(str(resp).encode())
                break

            # print(f"-- Operation doneeeee {server_info}")
            # Send the response to client or kvs or ecs
            self.request.sendall(str(resp).encode())
        # Close the client connection
        self.request.close()
        # print(f"Connection with {self.client_address[0]}:{self.client_address[1]} closed.")

    def receive_large_data(self):
        buffer_size = 1024
        received_data = b""

        while True:
            data = self.request.recv(buffer_size)
            if data is None:
                break
            received_data += data

            if '\r\n' in data.decode(): # receive data until it contains \r\n at the end
                break
        # print(received_data)
        return received_data




class MyRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # print(f"Connected to {self.client_address[0]}:{self.client_address[1]}")
        logging.info(f"Connected to {self.client_address[0]}:{self.client_address[1]}")
        logging.debug(f"this is just debug logs")

        welcome_message = "Welcome to the key value storage server!\r\n"
        self.request.sendall(welcome_message.encode())
        self.request.settimeout(20)
        while True:
            # Receive data from the client
            try:
                data = self.receive_large_data() # receive data from client (can be larger than 1024 bytes)
            except Exception as e:
                continue

            if not data:
                break

            resp = CommandHandler(data.decode('ascii'))

            # print(f"resp: {resp}")
            if resp == 'close_connection':
                break
            elif resp == 'data_ok':
                self.request.sendall(str(resp).encode())
                break

            # print(f"-- Operation doneeeee {server_info}")
            # Send the response to client or kvs or ecs
            self.request.sendall(str(resp).encode())
        # Close the client connection
        self.request.close()
        # print(f"Connection with {self.client_address[0]}:{self.client_address[1]} closed.")

    def receive_large_data(self):
        buffer_size = 1024
        received_data = b""

        while True:
            data = self.request.recv(buffer_size)
            if data is None:
                break
            received_data += data

            if '\r\n' in data.decode(): # receive data until it contains \r\n at the end
                break
        # print(received_data)
        return received_data


def ignoreWelcomeMessage(socket):
    buffer_size = 1024
    received_data = b""

    while True:
        data = socket.recv(buffer_size)
        received_data += data

        if '\r\n' in data.decode(): # receive data until it contains \r\n at the end
            break

def create_parser():
    # Create an argument parser
    parser = argparse.ArgumentParser(description='key Value Storage Server')

    # Add arguments
    parser.add_argument('-p', '--port', default=8000, type=int, help='Port that the server should use.')
    parser.add_argument('-a', '--address', default='localhost', help='Adress that server should use')
    parser.add_argument('-b', '--bootstrap', help='Bootstrap server')
    parser.add_argument('-d', '--directory', default='storage',
                        help='Directory that server should use for persistent storage')
    parser.add_argument('-l', '--log', default='logs/log.txt', help='Relative path of the logfile')
    parser.add_argument('-ll', '--loglevel', default='INFO', help='Set the log level of the server')
    parser.add_argument('-c', '--cache', type=int, default=20, help='Size of the cache')
    parser.add_argument('-s', '--strategy', default='LRU', help='Cache displacement strategy, FIFO, LRU, LFU')

    return parser


def connect_to_ecs(bootstrap, ip, port):
    global client_socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ecs_ip=bootstrap.split(':')[0]
    ecs_port=bootstrap.split(':')[1]
    try:
        # print(ecs_ip,ecs_port)
        client_socket.connect((ecs_ip, int(ecs_port)))
        # logging.info("Connected to server, waiting for reply...")
        # print("Connected to server, send join request")
        message= f'join {ip} {port}'
        client_socket.sendall(message.encode("ascii"))

    except Exception as e:
        logging.error(f"Failed to connect to server: {e}")
        client_socket = None

def shutdown_hook(bootstrap, ip , port):
    # print("shutdown hook started")
    global client_socket, isShuttingDown
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ecs_ip=bootstrap.split(':')[0]
    ecs_port=bootstrap.split(':')[1]
    try:
        isShuttingDown = True
        client_socket.connect((ecs_ip, int(ecs_port)))
        # print("Connected to server, waiting for reply...")
        # print("shutting down, sending shutdown request to ecs")
        message= f'shutdown {ip} {port}'
        client_socket.sendall(message.encode("ascii"))
        # print("waitin response from ecs")
        data = client_socket.recv(1024)
        data = data.decode('ascii').rstrip()
        logging.error(f"response data: {data}")
        if data == 'no_data_to_send\r\n':
            pass
            # print("no_data_to_send shutting down directly")
        else:
            # print("send data")
            CommandHandler(data)
            message= f'transfer_done'
            # print("sending transfer done")
            client_socket.sendall(message.encode("ascii"))
            operationsAfterTransferDone()
            # print("received some data")
            print(data)
            # print("operationsAfterTransferDone finished")
        # Remove all replicas and files
        remove_all_replicas()
    except Exception as e:
        print(f"Failed to connect to server: {e}")
        client_socket = None

class GracefulThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

def run_server(server):
    try:
        server.serve_forever()
    except Exception as e:
        print(f"Server error: {e}")
        logging.error(f"Server error: {e}")

def handle_shutdown_signal(signum, frame):
    # Call your shutdown_hook here
    shutdown_hook(args.bootstrap, args.address, args.port)
    # Stop the server
    # print("Stopping server")
    server.shutdown()
    # print("Server shutdown done")
    server.server_close()
    # print("Server closed")
    sys.exit(0)

def remove_kvs_and_files(server_info_variable):
    server_ip = None
    server_port = None

    if isinstance(server_info_variable, dict):
        server_ip = server_info_variable["ip"]
        server_port = server_info_variable["port"]
    elif isinstance(server_info_variable, key_value_store.KeyValueStore):
        server_ip = server_info_variable.ip
        server_port = server_info_variable.port
    else:
        raise TypeError("The argument must be a dictionary or an instance of KeyValueStore.")

    if server_ip is None or server_port is None:
        print("Error server_ip or server_port can not be None!")
        print("remove_kvs_and_files failed!")
        return

    folder_path = None
    for curr_kvs in kvs:
        if curr_kvs.is_ip_port_mine(server_ip, server_port):
            folder_path = curr_kvs.folder_path
            kvs.remove(curr_kvs)
            break
    if folder_path is not None:
        shutil.rmtree(folder_path)

def merge_data_from_replica(dict_server_info):
    merged_data = None
    for curr_kvs in kvs:
        if curr_kvs.is_ip_port_mine(dict_server_info["ip"], dict_server_info["port"]):
            merged_data = curr_kvs.get_all_data()
            break
    if merged_data is not None:
        # merged_data = json.loads(merged_data) # convert json string to python dictionary
        kvs[0].putTransferredData(merged_data)


def create_replica_kvs_and_put_data(ip, port, data):
    folder_path = args.directory + "/replica_" + str(ip) + "_" + str(port)
    kvs_replica = key_value_store.KeyValueStore(ip, port, args.cache, args.strategy, folder_path, False)
    kvs_replica.putTransferredData(data)
    kvs.append(kvs_replica)
    return kvs_replica

# Handle signals
signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)

if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    # print(args)

    server_info = {'ip': args.address, 'port': args.port, 'bootstrap': args.bootstrap} #set current server info to dict

    configure_logging(args.log, level=args.loglevel)
    kvs_me = key_value_store.KeyValueStore(args.address, args.port, args.cache, args.strategy, args.directory + "/me", True)
    kvs.append(kvs_me)

    if args.bootstrap is not None:
        connect_to_ecs(args.bootstrap, args.address, args.port)
        bootstrap = args.bootstrap

    # print(f"Started {server_info}")
    async_server_thread = threading.Thread(target=run_async_server)
    async_server_thread.start()

    with GracefulThreadingTCPServer((args.address, int(args.port)), MyRequestHandler) as server, \
        GracefulThreadingTCPServer((args.address, int(args.port) + 1000), MyHeartBeatRequestHandler) as heartbeat_server:        
        # Start the server in a separate thread
        server_thread = threading.Thread(target=run_server, args=(server,))
        server_thread.daemon = True  # This ensures that the thread will exit when the main program exits
        server_thread.start()

        # Start the server in a separate thread
        hearbeat_server_thread = threading.Thread(target=run_server, args=(heartbeat_server,))
        hearbeat_server_thread.daemon = True  # This ensures that the thread will exit when the main program exits
        hearbeat_server_thread.start()   

        print("Started")
        # time.sleep(10) # wait for server to start        
        # print("get test test")


        # Keep the main thread alive, waiting for termination signals
        while True:
            server_thread.join(1)
            hearbeat_server_thread.join(1)
            async_server_thread.join(1)