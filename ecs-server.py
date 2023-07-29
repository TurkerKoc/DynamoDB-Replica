import argparse
import logging
import socketserver
import os
import threading
import socket
import metaData
import signal
import time
import sys
# Global variables
meta_data = metaData.MetaData()
addRemoveLock = threading.Lock() # lock for adding and removing servers
server = None

def configure_logging(file_path, level=logging.INFO):
    # if not os.path.exists(os.path.join(os.getcwd(),file_path)):
    #     open(os.path.join(os.getcwd(),file_path), 'w').close()
    if args.loglevel in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FINEST"):
        pass
    else:
        print("Invalid log level. Use DEBUG, INFO, WARNING, ERROR or CRITICAL.")
        exit(1)
    log_dir = os.path.dirname(file_path)
    os.makedirs(os.path.join(os.getcwd(), log_dir), exist_ok=True)
    logging.basicConfig(
        level="DEBUG",
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(file_path),  # Specify the file name here
            # logging.StreamHandler()  # Optionally, log messages can also be displayed in the console
        ]
    )

def calculateWhoSendsWhom(ip, port, operation):
    #TODO calculate who sends data to whom
    if operation == 'join':
        # TODO calculate who sends data to whom for join operation
        # access formatted meta_data as meta_data.data
        if len(meta_data.ranges) == 1:
            return None, None, meta_data.ranges
        else:
            index=0
            for i in meta_data.ranges:
                if i['ip'] == ip and  i['port'] == port:
                    index=meta_data.ranges.index(i)
            # print("Index: " + str(index))
            if index == len(meta_data.ranges)-1:
                # print("")
                # print("Index: " + str(index))
                # print("")
                # print(meta_data.ranges[0]['ip'], meta_data.ranges[0]['port'], {"start": meta_data.ranges[index]['start'], "end": meta_data.ranges[index]['end']})
                return meta_data.ranges[0]['ip'], meta_data.ranges[0]['port'], {"start": meta_data.ranges[index]['start'], "end": meta_data.ranges[index]['end']}
            else:
                return meta_data.ranges[index+1]['ip'], meta_data.ranges[index+1]['port'], {"start": meta_data.ranges[index]['start'], "end": meta_data.ranges[index]['end']}


    elif operation == 'shutdown':
        # TODO calculate who sends data to whom for shutdown operation
        if len(meta_data.ranges) == 1:
            return None, None, meta_data.ranges # return dataSenderIp, dataSenderPort, keyRange
        else:
            index=0
            for i in meta_data.ranges:
                if i['ip'] == ip and  i['port'] == port:
                            index=meta_data.ranges.index(i)
            if index == len(meta_data.ranges)-1:
                return meta_data.ranges[0]['ip'], meta_data.ranges[0]['port'], {"start": meta_data.ranges[index]['start'], "end": meta_data.ranges[index]['end']}
            else:
                return meta_data.ranges[index+1]['ip'], meta_data.ranges[index+1]['port'], {"start": meta_data.ranges[index]['start'], "end": meta_data.ranges[index]['end']}


def updateMetaData(ip, port, operation):
    #TODO update meta data
    if(operation == 'join'):
        meta_data.add_server(ip, port)
        # print("")
        # print("Data: " + str(meta_data.data))
        # print("")
        # print("Ranges: " + str(meta_data.ranges))
        # print("")
        return calculateWhoSendsWhom(ip, port, operation)
    elif(operation == 'shutdown'):
        # print("Shutdown started")
        dataReceiverIp, dataReceiverPort, keyRange = calculateWhoSendsWhom(ip, port, operation)
        # print("Calculation done. removing server")
        meta_data.remove_server(ip, port)
        # print("Server removed")
        return dataReceiverIp, dataReceiverPort, keyRange
    

def sendUpdatedMetaDataToAllServers():
    #TODO send updated meta data to all servers
    # time.sleep(2)
    # global client_socket
    for i in meta_data.ranges:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((i['ip'], int(i['port'])))
        ignoreWelcomeMessage(client_socket) # ignore welcome message
        client_socket.sendall(f'metadata_broadcast  {str(meta_data.ranges)} \r\n'.encode())
        # confirmation = client_socket.recv(1024).decode('ascii')
        # logging.error(confirmation + ' by ' + i['ip'] + ' ' + i['port'])
    

def parseIpPort(command):
    ip = command.split(' ')[1]
    port = command.split(' ')[2]
    return ip, port


def ignoreWelcomeMessage(socket):
    buffer_size = 1024
    received_data = b""

    while True:
        data = socket.recv(buffer_size)
        received_data += data

        if '\r\n' in data.decode(): # receive data until it contains \r\n at the end
            break

def startPingingServer(ip, port):
    #TODO start pinging server
    # global client_socket
    global meta_data
    # print("Trying to connect to server ", ip, port)
    cur_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cur_client_socket.connect((ip, int(port)))
    # print("Connected to server ", ip, port)
    ignoreWelcomeMessage(cur_client_socket) # ignore welcome message
    # print("welcom message ignored")
    #ping pong with server to check if it is alive
    while any(ip == d['ip'] and port == int(d['port'])+1000 for d in meta_data.ranges): # while server is in meta data
        # print("Pinging server ", ip, port)
        cur_client_socket.sendall(f'ping\r\n'.encode())
        #if pong is not received in 700ms, server is dead
        cur_client_socket.settimeout(10)
        confirmation = cur_client_socket.recv(1024).decode('ascii').rstrip()
        # print(confirmation, "from ", ip, port)
        if confirmation == 'pong':
            time.sleep(1)
        else:
            # print("Server is dead. Shutting down")
            with addRemoveLock:
                # print("Lock acquired for non-responsive kvserver. Server is removing from the list.")
                updateMetaData(ip, str(port-1000), 'shutdown') # delete unresponsive server from meta data
                client_thread = threading.Thread(target=sendUpdatedMetaDataToAllServers)
                client_thread.start()
                # sendUpdatedMetaDataToAllServers() # send updated meta data to all servers
                break


class MyRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # print(f"New Server Connection {self.client_address[0]}:{self.client_address[1]}")
        logging.info(f"New Server Connection {self.client_address[0]}:{self.client_address[1]}")        

        # Receive data from the KVServer
        data = self.request.recv(1024)    
        if not data:
            return #TODO handle this   
        
        command = data.decode('ascii')

        command = command.rstrip() # remove trailing spaces
        operation = command.split(' ')[0] # get the operation
        if operation in ('join', 'shutdown'):
            with addRemoveLock: # lock for adding and removing servers (only one server at a time)
                logging.info(f"'{operation}' operation called")
                if operation == 'join': # new KVServer joins to cluster -> join IP PORT
                    try:
                        ip, port = parseIpPort(command) # get the ip and port of new server #do we need this ? dont we already have the ip and port
                        # print('ip', ip, port)
                        dataSenderIp, dataSenderPort, keyRange =  updateMetaData(ip, port, 'join') # get which server will send data to new server with the key range  
                        # print(dataSenderIp, dataSenderPort, keyRange)
                        # print(dataSenderIp, dataSenderPort, keyRange)                  
                        if (dataSenderIp and dataSenderPort): # if there is a server that will send data to new server
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # create connection to server that will send the data to new server and say transfer data
                                s.connect((dataSenderIp, int(dataSenderPort))) # connect to server that will send the data to new server
                                ignoreWelcomeMessage(s) # ignore welcome message
                                # print(ip)
                                # print(port)
                                # print(keyRange)
                                # print(keyRange['start'])
                                # print(keyRange['end'])
                                s.sendall(f"send_data {ip} {port} {keyRange['start']} {keyRange['end']} \r\n".encode()) # send_data newServerIp newServerPort keyRange
                            # start socket with dataSenderIp and dataSenderPort

                            responseFromNewServer = self.request.recv(1024) # wait from new server to say that it received the all the data
                            responseFromNewServer = responseFromNewServer.decode('ascii').rstrip() # remove trailing spaces
                            if responseFromNewServer != 'received_data':
                                logging.error(f"Error while sending data to new server")
                                #TODO return 'error while sending data to new server'
                        
                        # send updated meta_data to all servers
                        client_thread = threading.Thread(target=sendUpdatedMetaDataToAllServers)
                        client_thread.start()
                        # sendUpdatedMetaDataToAllServers() # send updated meta data to all servers            
                        # start pinging server
                        client_thread = threading.Thread(target=startPingingServer, args=(ip,int(port)+1000))
                        client_thread.start()
                    except Exception as e:
                        print(e)
                        # return 'join_error'
                elif operation == 'shutdown': # KVServer wants to shut down -> shutdown IP PORT
                    try:
                        # print("Shutdown request received")
                        ip, port = parseIpPort(command) # get the ip and port of new server
                        dataReceiverIp, dataReceiverPort, keyRange =  updateMetaData(ip, port, 'shutdown') # get which server will receive data from the server that will shut down with the key range

                        #
                        #
                        # IF new keyrange-array size EQUALS or GREATER than 2
                        #       Then: DO NOT TRANSFER DATA (ASSUME YOUR DATA IS ALREADY REPLICATED)
                        # ELIF new keyrange-array size SMALLER than 2 (it can be 1 or 0)
                        #       Then: Data can be transferred
                        #           (it will be transferred only if count equals to 1)
                        #           (if it is 0 then updateMetaData returns None values so transfer will not happen)
                        #
                        #
                        meta_data_new_count = len(meta_data.data)
                        if meta_data_new_count >= 2:
                            dataReceiverIp = None
                            dataReceiverPort = None

                        # print("Sending data to server")
                        # print(dataReceiverIp)
                        # print(dataReceiverPort)
                        # print(keyRange)
                        # print(keyRange['start'])
                        # print(keyRange['end'])
                        if(dataReceiverIp and dataReceiverPort): # if there is a server that will receive data from the server that will shut down
                            self.request.sendall(f"send_data {dataReceiverIp} {dataReceiverPort} {keyRange['start']} {keyRange['end']} \r\n".encode()) # send to server that will shut down that it will send data to another existsing server
                            # print("Data sent to " + dataReceiverIp + " " + dataReceiverPort)
                            responseFromShuttingDownServer = self.request.recv(1024) # wait from shutting down server to say that it sent the all the data to another server
                            # print("Response received")
                            responseFromShuttingDownServer = responseFromShuttingDownServer.decode('ascii').rstrip() # remove trailing spaces
                            if responseFromShuttingDownServer != 'transfer_done': # if server that will shut down did not send the data to another server
                                logging.error(f"Error while sending data to another server")
                                #TODO return 'error while sending data to new server'
                            # sendUpdatedMetaDataToAllServers() # send updated meta data to all servers            
                        else: # if there is no server that will receive data from the server that will shut down
                            self.request.sendall(f"no_data_to_send\r\n".encode())
                        
                        # print("Data sent to all servers")
                        # send updated meta_data to all servers                        
                        client_thread = threading.Thread(target=sendUpdatedMetaDataToAllServers)
                        client_thread.start()
                    except Exception as e:
                        print(e)
        else:
            logging.error(f"Invalid operation '{operation}' requested")
            return 'error unknown command!'


        # Close the client connection
        self.request.close()
        # print(f"Connection with {self.client_address[0]}:{self.client_address[1]} closed.")


def create_parser():
    # Create an argument parser
    parser = argparse.ArgumentParser(description='ECS Server')

    # Add arguments
    parser.add_argument('-p', '--port', default=8000, type=int, help='Port that the server should use.')
    parser.add_argument('-a', '--address', default='localhost', help='Adress that server should use')
    parser.add_argument('-l', '--log', default='logs/log.txt', help='Relative path of the logfile')
    parser.add_argument('-ll', '--loglevel', default='INFO', help='Set the log level of the server')


    return parser


def shutdown_hook():
    pass
    # print("shutdown hook started")

class GracefulThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

def run_server(server):
    try:
        server.serve_forever()
    except Exception as e:
        # print(f"Server error: {e}")
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

# Handle signals
signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)


if __name__ == "__main__":    
    parser = create_parser()
    args = parser.parse_args()

    # print(args)

    configure_logging(args.log, level=args.loglevel)

    # print("Started")


    with GracefulThreadingTCPServer((args.address, int(args.port)), MyRequestHandler) as server:

        # Start the server in a separate thread
        server_thread = threading.Thread(target=run_server, args=(server,))
        server_thread.daemon = True  # This ensures that the thread will exit when the main program exits
        server_thread.start()

        # Keep the main thread alive, waiting for termination signals
        while True:
            server_thread.join(1)