import threading
import socket
import pika
import requests
import sys
import json

server = input("Enter server url: ")
dataNode_ip = int(sys.argv[1])

filesBlocks = []
rabbitmq_port =5672 
rabbitmq_ip ='23.20.131.49'

def callback(ch, method, properties, body):
    headers = properties.content_type.split(",")
    print(body)
    fileBlockInfo = {
        'fileName': headers[0],
        'blockPosition': headers[2],
        'blockContent': body
    }
    filesBlocks.append(fileBlockInfo)

    url = server + 'indexFiles' 
    indexFilesFields = {
        'fileName': headers[0],
        'dataNodeIp': dataNode_ip,
        'blockPosition': int(headers[2]),
        'totalBlocks': int(headers[3])
    }
    
    response = requests.post(url, json=indexFilesFields)

    if response.status_code == 200:
        print("Message sent successfully to Server A")
    else:
        print("Failed to send message to Server A")

def consume_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_ip,rabbitmq_port))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
    print(f'[*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def server_thread(ip, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip, int(port)))
    server_socket.listen(10) 
    print(f"Server is listening on {ip}:{port}")

    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Received connection from {client_address}")
        response = client_socket.recv(1024).decode()
        targetBlock = json.loads(response)

        for block in filesBlocks:
            print(block["fileName"], targetBlock['fileName'], block["blockPosition"], targetBlock['blockPosition'] )
            if str(block["fileName"]) == str(targetBlock['fileName']) and int(block["blockPosition"]) == int(targetBlock['blockPosition']):
                print("First occurrence found:")
                client_socket.send(block["blockContent"])
                client_socket.close()
                break


def main():
    ip = '127.0.0.1' 
    server_thread_obj = threading.Thread(target=server_thread, args=(ip, dataNode_ip))
    server_thread_obj.start()

    consume_messages()

if __name__ == "__main__":
    main()