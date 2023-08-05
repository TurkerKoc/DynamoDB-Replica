import csv
import subprocess
import sys
import threading
import time
import json
from threading import Thread
import psutil
import hashlib
from socket import socket, timeout
from datetime import datetime
import os
import signal
import generateDataset
import websockets
import asyncio
import logging



# Define your signal handler functions
def handle_sigint(sig, frame):
    print("SIGINT (Ctrl+C) received!")
    # Here you can also do any cleanup before exiting
    sys.exit(0)  # Exit cleanly with a 0 status


def handle_sigterm(sig, frame):
    print("SIGTERM received!")
    # Here you can also do any cleanup before exiting
    sys.exit(0)  # Exit cleanly with a 0 status


# Register your signal handlers
signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigterm)

script_dir = os.path.dirname(os.path.realpath(__file__))  # gets the directory where the script is located
benchmarking_folder = os.path.join(script_dir, '..', 'benchmarking_results')
ecs_kvs_folder = os.path.join(script_dir, '..')

# Create the directory if it doesn't exist
if not os.path.exists(benchmarking_folder):
    os.makedirs(benchmarking_folder)


class ClientThread(Thread):
    def __init__(self, params, key_value_set, stop_event):
        Thread.__init__(self)
        self.params = params
        self.key_value_set = key_value_set
        client_id = hashlib.sha256(json.dumps(key_value_set).encode()).hexdigest()
        self.name = f'Client {client_id}'
        self.results_file = f'{benchmarking_folder}/results_{client_id}.csv'
        self.stop_event = stop_event
        print(f'Initialized {self.name} thread with result file: {self.results_file}')

    def run(self):
        print(f'Starting {self.name} thread...')
        client = None
        try:
            with open(self.results_file, 'a', newline='') as file:
                writer = csv.writer(file)
                for dictionary in self.key_value_set:
                    key = dictionary['key']
                    value = dictionary['value']
                    # Check the stop event here
                    if self.stop_event.is_set():
                        print(f'Stopping {self.name} thread...')
                        return

                    # Connect to the first KVServer
                    client = SocketClient(host=self.params['host'], port=self.params['port_start'])
                    client.connect()

                    # Measure the latency and throughput for PUT, GET, and DELETE operations
                    for operation in ['put', 'get', 'delete']:
                        # Check the stop event here
                        print(f'Running {operation} operation...')
                        if self.stop_event.is_set():
                            print(f'Stopping {self.name} thread...')
                            return

                        durations = []  # Collect durations for each operation
                        successful_requests = 0  # Track successful requests

                        for _ in range(self.params['n_requests']):
                            # Check the stop event here
                            if self.stop_event.is_set():
                                print(f'Stopping {self.name} thread...')
                                return

                            while True:  # Keep trying until operation is successful
                                if self.stop_event.is_set():  # Check the stop event here too
                                    print(f'Stopping {self.name} thread...')
                                    return

                                command = f"{operation} {key}"
                                if operation == 'put':
                                    command += f" {value}"

                                command += '\r\n'
                                print(f"Sending command: {command}")
                                start_time = time.time()
                                client.send(command)
                                response = client.receive()
                                end_time = time.time()
                                # print(f"Received response: {response}")
                                # print(f"Operation started at {start_time}")
                                # print(f"Operation ended at {end_time}")
                                # print(f"Operation took {end_time - start_time} seconds")

                                if "server_not_responsible" in response:
                                    client.send("keyrange"+'\r\n')
                                    keyrange_response = client.receive()
                                    servers = client.handle_keyrange_response(keyrange_response)
                                    responsible_server = client.find_responsible_server(servers, key)

                                    client.close()
                                    client = SocketClient(host=responsible_server["ip"],
                                                          port=responsible_server["port"])
                                    client.connect()
                                    continue  # Retry the operation

                                if "server_stopped" in response or "_error" in response:
                                    # TODO handle server stopped or error
                                    break

                                durations.append(end_time - start_time)
                                successful_requests += 1  # Increment successful requests
                                break  # Operation was successful, move to next request

                        print(f"Successful requests for {operation} operation: {successful_requests}")
                        print(f"Durations for {operation} operation: {durations}")
                        # Calculate the average latency and throughput
                        avg_latency = sum(durations) / len(durations)
                        print(f"Sum of durations for {operation} operation: {sum(durations)}")
                        throughput = successful_requests / sum(durations)

                        print(f"Latency for {operation} operation: {avg_latency} seconds")
                        print(f"Throughput for {operation} operation: {throughput} operations/second")

                        writer.writerow([self.name, operation, 'latency', avg_latency])
                        writer.writerow([self.name, operation, 'throughput', throughput])
                        # break

                    client.close()
        finally:
            # Ensure that the client connection is always closed
            if client is not None:
                client.close()
            print(f'Finished {self.name} thread.')


# Helper class for socket communication
class SocketClient:
    def __init__(self, host='localhost', port=8001):
        self.host = host
        self.port = port
        self.socket = socket()

    def connect(self):
        self.socket.settimeout(10)  # Set timeout to 10 seconds
        self.socket.connect((self.host, self.port))
        # Discard welcome message
        self.receive()
        print(f'Connected to server at {self.host}:{self.port}')

    def send(self, data):
        self.socket.sendall(data.encode())

    def receive(self):
        buffer_size = 1024
        received_data = b""

        while True:
            data = None

            try:
                # print(f"Waiting for data...")
                data = self.socket.recv(buffer_size)
                # print(f"Received data: {data.decode()}")
                cond = '\r\n' in data.decode()
                # print(f"Condition: {cond}")
            except timeout:
                print(f"Server did not respond in time.")
                pass

            if not data or data is None:
                break
            received_data += data

            if '\r\n' in data.decode():  # receive data until it contains \r\n at the end
                break

        print(f'Received data: {received_data.decode()}')
        return received_data.decode()

    def handle_keyrange_response(self, response):
        # Example response: "keyrange_success start1,end1,ip1:port1;start2,end2,ip2:port2;\r\n"
        servers_info = response[len("keyrange_success "):-2].split(";")  # Ignore the prefix and the last '\r\n'
        servers = [self.parse_server_info(info) for info in servers_info if info]
        return servers

    def parse_server_info(self, info):
        # Example info: "start1,end1,ip1:port1"
        start, end, server = info.split(",")
        ip, port = server.split(":")
        return {
            "start": start,  # Keep the hexadecimal format
            "end": end,  # Keep the hexadecimal format
            "ip": ip,
            "port": int(port)
        }
    
    def compute_md5_hash(self, string):
        md5_hash = hashlib.md5()
        md5_hash.update(string.encode('utf-8'))
        return md5_hash.hexdigest()


    def hexToInt(self, hex_string):
        return int(hex_string, 16)

    def find_responsible_server(self, servers, key):
        # key is converted to MD5 hash and compared in hexadecimal format
        key = self.compute_md5_hash(key)
        for server in servers:
            if self.hexToInt(server['start']) <= self.hexToInt(key) <= self.hexToInt(server['end']) or (self.hexToInt(server['start']) > self.hexToInt(server['end']) and (self.hexToInt(key) >= self.hexToInt(server['start']) or self.hexToInt(key) <= self.hexToInt(server['end']))): # right side of or for the first node in the ring
                return server
        return None

    def close(self):
        self.socket.close()

async def run_client(port):
    websocket_port = port + 100
    websocket = await websockets.connect(f"ws://localhost:{websocket_port}")
    await websocket.send(json.dumps({'action': 'subscribe', 'type': 'operation', 'name': 'get'}))
    await websocket.send(json.dumps({'action': 'subscribe', 'type': 'operation', 'name': 'put'}))
    await websocket.send(json.dumps({'action': 'subscribe', 'type': 'operation', 'name': 'delete'}))

    while True:
            message = await websocket.recv()
            # print(f"Received message: {message}")

def start_run_client_in_thread(port):
    asyncio.run(run_client(port))


def read_dataset_from_csv(filename):
    dataset = []
    with open(filename, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dataset.append(row)
    return dataset

n_clients = 1  # Number of clients
client_datasets = []
for i in range(n_clients):
    client_datasets.append(read_dataset_from_csv('data_10.csv'))
# print(client_datasets)
# Define the parameters
params = {
    'n_servers': 4,  # Number of KVServer instances
    'n_requests': 1,  # Number of requests per key
    'n_subscribers': 5,
    'host': 'localhost',
    'port_start': 8001,  # Starting port for the KVServer instances
    'keys': client_datasets
    # Example keys
    # 'keys': [

    #     [{'key': 'key1', 'value': 'value1'}, {'key': 'key2', 'value': 'value2'},{'key': 'key3', 'value': 'value3'}],
    #     [{'key': 'key11', 'value': 'value11'}, {'key': 'key22', 'value': 'value22'},{'key': 'key33', 'value': 'value33'}],
    #     [{'key': 'key111', 'value': 'value111'}, {'key': 'key222', 'value': 'value222'}, {'key': 'key333', 'value': 'value333'}]
    #                 # , {'key': 'key2', 'value': 'value2'},{'key': 'key3', 'value': 'value3'}
    # ]
}
print('Defined parameters for benchmarking...')
print(params)

# Prepare the test
servers = []
server_pids = []
ecs = None
threads = []
monitor_thread = None
stop_event = threading.Event()
stop_monitoring = False
subscribe_client_threads = []

try:
    # Start the ECS
    print('Starting ECS...')
    ecs = subprocess.Popen(["python", f"{ecs_kvs_folder}/ecs-server.py"])
    ecs_process = psutil.Process(ecs.pid)
    time.sleep(5)  # Allow ECS to fully start
    if ecs.poll() is not None:
        print('ECS failed to start.')
        sys.exit(1)  # or any non-zero exit code you prefer
    print('ECS started.')

    # Start the KVServer instances
    print('Starting KVServer instances...')
    loop = asyncio.get_event_loop()
    for i in range(params['n_servers']):
        port = params['port_start'] + i
        server = subprocess.Popen(
            ["python", f"{ecs_kvs_folder}/kv_server.py", "-p", str(port), "-b", f"{params['host']}:8000", "-d",
             f"storage{i + 1}", "-l", f"logs/server{i + 1}.txt", "-ll", "DEBUG"])
        servers.append(server)
        server_pids.append(server.pid)
        time.sleep(5)  # Allow server to fully start

        if server.poll() is not None:
            print(f'KVServer instance {i} failed to start.')
            sys.exit(1)


        for i in range(params['n_subscribers']):
            # thread = threading.Thread(target=loop.run_until_complete, args=(run_client(port),))
            thread = threading.Thread(target=start_run_client_in_thread, args=(port,))
            thread.start()
            subscribe_client_threads.append(thread)

        print(f'KVServer instance {i} started.')

    server_processes = [psutil.Process(server.pid) for server in servers]

    # Initialize the lists to store the cpu and memory usage data
    ecs_cpu_usage = []
    ecs_memory_usage = []
    server_cpu_usage = [[] for _ in range(params['n_servers'])]
    server_memory_usage = [[] for _ in range(params['n_servers'])]


    # Start a separate thread to monitor system usage
    def monitor_usage():
        while not stop_monitoring:
            # You can measure the CPU and memory usage percentage of each process
            ecs_cpu = ecs_process.cpu_percent(interval=1)
            ecs_memory = ecs_process.memory_info().rss / (1024 ** 2)  # Convert to MB
            ecs_cpu_usage.append(ecs_cpu)
            ecs_memory_usage.append(ecs_memory)

            for i, server_process in enumerate(server_processes):
                try:
                    server_cpu = server_process.cpu_percent(interval=1) / psutil.cpu_count()  # Convert to percentage
                    server_memory = server_process.memory_info().rss / (1024 ** 2)  # Convert to MB
                    server_cpu_usage[i].append(server_cpu)
                    server_memory_usage[i].append(server_memory)
                except psutil.NoSuchProcess:
                    pass

            time.sleep(1)  # Adjust the interval as needed


    # Start the monitoring thread
    print('Starting monitoring thread...')
    monitor_thread = Thread(target=monitor_usage)
    monitor_thread.start()
    print('Monitoring thread started.')

    # Run the tests
    print('Running tests...')
    start_time = datetime.now()

    for key_value_set in params['keys']:
        thread = ClientThread(params, key_value_set, stop_event)
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    print('Tests finished.')

    # Signal the monitoring thread to stop and wait for it
    print('Stopping monitoring thread...')
    stop_monitoring = True
    monitor_thread.join()
    print('Monitoring thread stopped.')

    # After stopping the monitoring thread...
    # Save the CPU and memory usage data to CSV files
    print('Saving CPU and memory usage data...')
    with open(f'{benchmarking_folder}/ecs_cpu_usage.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time', 'usage'])
        for i, usage in enumerate(ecs_cpu_usage):
            writer.writerow([i, usage])

    # Save the CPU and memory usage data to CSV files
    for i in range(params['n_servers']):
        with open(f'{benchmarking_folder}/server{i}_cpu_usage.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['time', 'usage'])
            for j, usage in enumerate(server_cpu_usage[i]):
                writer.writerow([j, usage])

        with open(f'{benchmarking_folder}/server{i}_memory_usage.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['time', 'usage'])
            for j, usage in enumerate(server_memory_usage[i]):
                writer.writerow([j, usage])
    print('Saved CPU and memory usage data.')

    for thread in subscribe_client_threads:
        # Wait for the thread to finish
        thread.join()

    # Wait for a while to make sure everything is finished
    print('Waiting for processes to finish...')
    time.sleep(5)

    # Send SIGINT signal to KVServer instances
    print('Sending SIGINT to KVServer instances...')
    for server in servers:
        os.kill(server.pid, signal.SIGINT)
        time.sleep(5)
        os.kill(server.pid, signal.SIGINT)
        time.sleep(8)

    # Send SIGINT signal to ECS
    print('Sending SIGINT to ECS...')
    os.kill(ecs.pid, signal.SIGINT)
    time.sleep(5)
    os.kill(ecs.pid, signal.SIGINT)
    time.sleep(8)
    print('Sent SIGINT to all processes.')

    # Print the total test duration
    end_time = datetime.now()
    print(f"Total test duration: {end_time - start_time}")
except Exception as e:
    print(f"An error occurred: {e}")
    # You can handle different exceptions here if needed
finally:
    print("Cleaning up threads and subprocesses...")

    # Stop the threads
    if threads:
        for thread in threads:
            if thread.is_alive():
                stop_event.set()

    # Stop the monitoring thread
    if monitor_thread and monitor_thread.is_alive():
        # Signal the monitoring thread to stop here if it's not done yet
        stop_monitoring = True
        monitor_thread.join()

    # Stop the KVServer instances
    # First send SIGTERM to give them a chance to exit gracefully
    print('Sending SIGINT to KVServer instances...')
    for server in servers:
        os.kill(server.pid, signal.SIGINT)
        time.sleep(5)
        os.kill(server.pid, signal.SIGINT)
        time.sleep(8)

    print('Sending SIGINT to ECS...')
    os.kill(ecs.pid, signal.SIGINT)
    time.sleep(5)
    os.kill(ecs.pid, signal.SIGINT)
    time.sleep(8)
    print('Sent SIGINT to all processes.')

    # Then terminate them if they are still running
    if servers:
        for server in servers:
            try:
                server.terminate()
                server.wait(timeout=10)
            except Exception as e:
                print(f"Failed to stop server: {e}")

    if ecs:
        try:
            ecs.terminate()
            ecs.wait(timeout=10)
        except Exception as e:
            print(f"Failed to stop ECS: {e}")

    print("Cleanup complete.")
