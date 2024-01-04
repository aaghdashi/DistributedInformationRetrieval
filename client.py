import socket
import threading
from colored import fg


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

data = []

# Search for data in local datas
def execute_query(query):
    result = []
    field = query['field']
    value = query['value']

    for d in data:
        if field in d and value.lower() == d[field].lower():
            result.append(d)
            
    return result


# Handle received messages from server
def message_handler(sock):
    while True:
        message = receive_all(sock).decode()
        message = eval(message)

        if message['type'] == 'query':
            query = message['data']
            result = execute_query(query)
            message['data'] = result
            message['type'] = 'result'
            color = fg('red')
            print(color+"records that found in this client:\n "+ str(result))
            
            sock.sendall(str(message).encode())

        elif message['type'] == 'result':
            result = message['data']
            if not result:
                print("Not found")
            else:
                for res in result:
                    color = fg('blue')
                    print(color+str(res))

        elif message['type'] == 'data':
            global data
            result = message['data']
            data = result
            # print(data)


# Send query to server
def send_query(sock):
    color = fg('green')
    print(color+"To search, enter the information like below: \n For example \n firtname=Mike \n lastname =Hessel \n id=101919 \n city=Alizaton ")
    color = fg('red')
    print(color+"exit for termination")
    color = fg('white')
    message = input(color+" -> ") 

    while message != 'exit':
        msg = {'type': 'query', 'data':message}
        sock.sendall(str(msg).encode()) 
        message = input() 

    sock.close()  


# Create a connection to server
def connect():
    host = socket.gethostname() 
    port = 5000  
    sock = socket.socket()  
    sock.connect((host, port)) 
    return sock


if __name__ == '__main__':
    sock = connect()
    threading.Thread(target=message_handler, args=(sock,)).start()
    send_query(sock)
