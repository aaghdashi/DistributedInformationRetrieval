import socket
import csv
import threading
import logging as log
from random import choice as randchoice
import traceback
import re

log.basicConfig(level = log.INFO,format='%(asctime)s - %(levelname)s - %(message)s')


clients = []
results = {}


# Helper function to get big amount of data through socket
def receive_all(sock):      
    BUFF_SIZE = 4096 
    data = b''
    while True:
        part = sock.recv(BUFF_SIZE)
        data += part
        if len(part) < BUFF_SIZE:
            break
    return data

# read data from csv file 
def read_data():
    with open('RandomData.csv') as f:
        data = list(csv.DictReader(f, quotechar="'",))
        return data

data = read_data()

# Send random parts of data to active clients
def distribute_data():
    if len(clients) > 0:
        parts = [ [] for i in range(len(clients))]      
        for d in data:
            randchoice(parts).append(d)

        for i in range(len(clients)):
            message = {'type':'data', 'data':parts[i]}
            clients[i].sendall(str(message).encode())


# add client to list of active clients
def on_connect(clientsocket):
    log.info(f'Client Connected: {clientsocket.getpeername()}')
    clients.append(clientsocket)
    distribute_data()


# remove client from list of active clients
def on_disconnect(clientsocket):
    log.info(f'Client Disconnected: {clientsocket.getpeername()}')
    clientsocket.close()
    clients.remove(clientsocket)
    distribute_data()


# handle received messages over socket
def on_message(sock, message):
    try:
        if message['type'] == 'query':
            log.info(f"Received query '{message['data']}' from client {sock.getpeername()}")
            term = re.findall(r'(\w+)\s*=\s*(\w+)', message['data'])[0]
            query = {'field':term[0], 'value': term[1]}
            id = hash(sock.getpeername())
            msg = {'type':'query', 'data':query, 'id': id}
            results[id] = {'responds':0, 'data':[]}
            for clientsocket in clients:
                clientsocket.send(str(msg).encode())

        elif message['type'] == 'result':
            result = message['data']
            log.info(f"Received {len(result)} results from client {sock.getpeername()}")
            results[message['id']]['responds'] += 1
            results[message['id']]['data'].extend(result)
            # print(message['id'])
            if results[message['id']]['responds'] >= len(clients):
                for clientsocket in clients:
                    if 'id' in message and hash(clientsocket.getpeername()) == message['id']:
                        msg = {'type':'result', 'data':results[message['id']]['data']}
                        clientsocket.sendall(str(msg).encode())
                        del results[message['id']]
                        
    except Exception as e:
        log.warning(e)


# Main part of code that calls above functions based on client actions
def client_handler(clientsocket):
    on_connect(clientsocket)
    while True:
        if clientsocket in clients:
            message = receive_all(clientsocket).decode()
            
            if not message:
                on_disconnect(clientsocket)
            else:
                #log.info(f"{addr} >>  {message}")
                message = eval(message)
                on_message(clientsocket, message)       



if __name__ == "__main__":

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    host = socket.gethostname() 
    port = 5000                

    log.info('Server started!')
    print(host,port)
    s.bind((host, port))        
    s.listen(1000)                 


    while True:
        c, addr = s.accept()         #Wait for clients
        thread = threading.Thread(target=client_handler, args=(c,))     # start new thread on background for each client
        thread.start()
   
   



    