import socket
import threading
import websockets
import asyncio
import json
import argparse
import time
import sys

parser = argparse.ArgumentParser(description='Client for Key Value Storage Server')
args = parser.parse_args()


class Client:
    def __init__(self):
        self.socket = None
        self.websocket = None
        self.socket_port = None
        self.websocket_port = None
        self.connected = False
        self.stop_receiving = asyncio.Event()

    async def connect(self, port):
        if self.connected:
            print("Already connected. Please disconnect before trying to connect again.")
            return

        self.socket_port = port
        self.websocket_port = port + 100
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(('localhost', self.socket_port))
        # dismiss welcome message
        data = self.socket.recv(1024)
        print(data.decode(), end="", flush=True)
        self.websocket = await websockets.connect(f"ws://localhost:{self.websocket_port}")
        self.connected = True
        self.stop_receiving.clear()
        asyncio.ensure_future(self.receive_messages())

    def disconnect(self):
        if self.connected:
            self.socket.close()
            asyncio.create_task(self.websocket.close())
            self.connected = False
            self.stop_receiving.set()  # add this line
            print("Successfully disconnected from server.")
        else:
            print("Not currently connected to any server.")

    def socket_client(self, command):
        command = command.rstrip()
        command += '\r\n'
        self.socket.sendall(command.encode())
        data = (self.socket.recv(1024)).decode().rstrip()
        # print('Received', repr(data))
        sys.stdout.write('Received ' + repr(data) + '\n')
        sys.stdout.flush()

    async def receive_messages(self):
        while not self.connected:
            pass
        while self.connected and not self.stop_receiving.is_set():
            try:
                message = await self.websocket.recv()
            except websockets.ConnectionClosed:
                break
            time.sleep(0.5)
            sys.stdout.write('\r\033[K')
            sys.stdout.flush()
            print(f"Received via WebSocket: {message}")
            # Print the input prompt on the next line
            self.print_prompt()

    async def handle_command(self, command):
        cmd_list = command.split()

        if len(cmd_list) == 0:
            print("Invalid command format.")
            return

        if not self.connected and cmd_list[0].lower() != "connect":
            print("You need to connect to the server first. Use 'connect IP PORT_NUMBER'")
            return
        if cmd_list[0].lower() == "connect":
            if len(cmd_list) == 3 and cmd_list[1] == "localhost" and cmd_list[2].isdigit():
                await self.connect(int(cmd_list[2]))
            else:
                print("Invalid command format. Correct format: 'connect IP PORT_NUMBER'")
        elif cmd_list[0].lower() == "disconnect":
            self.disconnect()
        elif cmd_list[0].lower() in ['subscribe', 'unsubscribe']:
            if len(cmd_list) == 3 and (cmd_list[1] == "key" or cmd_list[1] == "operation"):
                await self.websocket.send(
                    json.dumps({'action': cmd_list[0].lower(), 'type': cmd_list[1].lower(), 'name': cmd_list[2]}))
            else:
                print("Invalid command format. Correct format: 'connect localhost PORT_NUMBER'")
        else:
            self.socket_client(command)

    def print_enter_command(self):
        print("Enter your command: ", end="")
        sys.stdout.flush()

    def print_prompt(self):
        # Move cursor to the beginning of the line, clear it and print prompt
        sys.stdout.write('\r\033[K')  # \r returns to the start of the line. \033[K clears the line
        sys.stdout.write("Enter your command: ")
        sys.stdout.flush()  # ensure it's displayed immediately

    def input_thread(self, loop):
        # asyncio.run_coroutine_threadsafe(self.handle_command("connect localhost 8001"), loop)
        # time.sleep(0.5)
        # asyncio.run_coroutine_threadsafe(self.handle_command("subscribe key x"), loop)
        # time.sleep(0.5)
        command = ""
        while True:
            self.print_enter_command()
            self.print_prompt()
            command = input()
            asyncio.run_coroutine_threadsafe(self.handle_command(command), loop)
            time.sleep(0.5)

    def start(self):
        loop = asyncio.get_event_loop()
        input_thread = threading.Thread(target=self.input_thread, args=(loop,))
        input_thread.start()

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()


client = Client()
client.start()
