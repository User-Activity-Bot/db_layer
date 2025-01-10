import os
import logging
import zmq

from dotenv import find_dotenv, load_dotenv

from functions import Functions
from scylladb_client import ScyllaDBClient

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("server.log"),
        logging.StreamHandler()
    ]
)

SCYLLA_HOSTS = os.getenv("SCYLLA_HOSTS").split(",")
SCYLLA_HOSTS = [host.strip() for host in SCYLLA_HOSTS if host.strip()]
KEYSPACE = os.getenv("KEYSPACE")

db_client = ScyllaDBClient(SCYLLA_HOSTS, KEYSPACE)

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

logging.info("ZeroMQ сервер запущен и слушает на tcp://*:5555")

while True:
    try:
        message = socket.recv_json()
        logging.info(f"Получено сообщение: {message}")
        functions = Functions(db_client=db_client)
        response = functions.handle_request(message)
        socket.send_json(response)
    except Exception as e:
        error_msg = f"Ошибка: {str(e)}"
        logging.error(error_msg)
        socket.send_json({"error": error_msg})