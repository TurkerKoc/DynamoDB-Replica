import socket
import logging
import argparse
import sys

client_socket = None


class InputAwareStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            stream = self.stream
            msg = self.format(record)
            stream.write('\x1b[A')  # Move up one line
            stream.write('\x1b[K')  # Clear the current line
            stream.write(msg)
            stream.write("\n")
            # stream.write("EchoClient> ")  # Reprint the input prompt
            stream.flush()
        except Exception:
            self.handleError(record)


def configure_logging(level=logging.ERROR):
    logging.basicConfig(level=level)
    root_logger = logging.getLogger()
    root_logger.handlers = []  # Remove default handlers
    handler = InputAwareStreamHandler(sys.stdout)
    formatter = logging.Formatter('EchoClient> %(levelname)s! %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


class CustomArgParser(argparse.ArgumentParser):
    def error(self, message):
        raise SystemExit


def cmd_connect(host, port):
    global client_socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((host, port))
        # logging.info("Connected to server, waiting for reply...")
        logging.error("Connected to server, waiting for reply...")
        confirmation = client_socket.recv(128 * 1024).decode("ascii")
        if confirmation.endswith('\r\n'):
            confirmation = confirmation[:-2]
        # logging.info(f"{confirmation}")
        logging.error(f"{confirmation}")
    except Exception as e:
        logging.error(f"Failed to connect to server: {e}")
        client_socket = None


def cmd_disconnect():
    global client_socket
    if client_socket:
        client_socket.close()
        logging.info("Disconnected from server.")
        client_socket = None
    else:
        logging.error("Not connected !")


def cmd_send_message(message):
    message += "\r\n"
    logging.info(f"Sending client message: '{message}'")
    global client_socket
    if client_socket:
        client_socket.sendall(message.encode("ascii"))
        response = client_socket.recv(128 * 1024).decode("ascii")
        if response.endswith('\r\n'):
            response = response[:-2]
        # logging.info(f"Received message from server: '{response}'")
        logging.error(f"EchoClient> {response}")
    else:
        logging.error("Not connected !")


def cmd_quit():
    exit()


def cmd_loglevel(level):
    if level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        logging.getLogger().setLevel(getattr(logging, level))
    else:
        logging.error("Invalid log level. Use DEBUG, INFO, WARNING, ERROR or CRITICAL.")


def create_parser():
    parser = CustomArgParser(description="TCP echo client", add_help=False)
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("quit", help="Exit the program")
    subparsers.add_parser("help", help="Show available commands")
    subparsers.add_parser("disconnect", help="Disconnect from the server")

    log_level_parser = subparsers.add_parser("logLevel", help="Set the logging level")
    log_level_parser.add_argument("level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                    help="Logging level")

    connect_parser = subparsers.add_parser("connect", help="Connect to the server")
    connect_parser.add_argument("host", help="Server host")
    connect_parser.add_argument("port", type=int, help="Server port")

    send_parser = subparsers.add_parser("send", help="Send a message to the server")
    send_parser.add_argument("message", nargs=argparse.REMAINDER, help="Message to send")

    set_parser = subparsers.add_parser("put", help="Send a message to the server")
    set_parser.add_argument("message", nargs=argparse.REMAINDER, help="Message to send")

    get_parser = subparsers.add_parser("get", help="Send a message to the server")
    get_parser.add_argument("message", nargs=argparse.REMAINDER, help="Message to send")

    update_parser = subparsers.add_parser("update", help="Send a message to the server")
    update_parser.add_argument("message", nargs=argparse.REMAINDER, help="Message to send")

    delete_parser = subparsers.add_parser("delete", help="Send a message to the server")
    delete_parser.add_argument("message", nargs=argparse.REMAINDER, help="Message to send")
    
    keyrange_parser = subparsers.add_parser("keyrange", help="Send a message to the server") #getting meta-data from kv-server    
    keyrange_read_parser = subparsers.add_parser("keyrange_read", help="Send a message to the server") #getting meta-data from kv-server    

    return parser


def cli():
    parser = create_parser()
    # logging.error("Type 'help' for available commands.")
    while True:
        command = input("EchoClient> ")
        logging.debug(f"Command: '{command}'")

        try:
            args = parser.parse_args(command.split())
        except SystemExit:
            # SystemExit exception raised by parser.parse_args() when it encounters an error.
            # logging.error(f"Parse error !")
            parser.print_help()
            continue

        if args.command == "quit":
            cmd_disconnect()
            exit()
        elif args.command == "help":
            parser.print_help()
        elif args.command == "logLevel":
            cmd_loglevel(args.level)
        elif args.command == "connect":
            cmd_connect(args.host, args.port)
        elif args.command == "disconnect":
            cmd_disconnect()
        elif args.command == "send":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message(" ".join(args.message))
        elif args.command == "put":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message("put " + " ".join(args.message))
        elif args.command == "get":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message("get " + " ".join(args.message))
        elif args.command == "update":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message("update " + " ".join(args.message))
        elif args.command == "delete":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message("delete " + " ".join(args.message))
        elif args.command == "keyrange":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message("keyrange")
        elif args.command == "keyrange_read":
            # args.message returns a list of words seperated by a comma, so we need to concatenate
            cmd_send_message("keyrange_read")


if __name__ == "__main__":
    configure_logging()
    cli()
