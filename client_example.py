import zmq
import datetime

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

def create():
    start = datetime.datetime.now()
    data = {"action" : "create_document", "username": "@not_none", "status": "online"}
    socket.send_json(data)
    response = socket.recv_json()
    print(response)
    end = datetime.datetime.now() - start
    print(end)
    
def get():
    start = datetime.datetime.now()
    data = {"action" : "get_document", "username": "bob", "status" : "online"}
    socket.send_json(data)
    response = socket.recv_json()
    print(response)
    end = datetime.datetime.now() - start
    print(end)