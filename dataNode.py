import threading
import socket
import pika
import requests
import sys
import json

server = input("Enter server url: ")
dataNode_ip = int(sys.argv[1])

filesBlocks = []
rabbitmq_port = 5672 
rabbitmq_ip = '3.226.23.240'

def callback(ch, method, properties, body):
    headers = properties.content_type.split(",")
    fileBlockInfo = {
        'fileName': headers[0],
        'blockPosition': headers[2],
        'blockContent': body
    }
    filesBlocks.append(fileBlockInfo)
    print("Fragment added to filesBlocks","Nombre Archivo:",fileBlockInfo['fileName'],"Numero Fragmento; ", fileBlockInfo['blockPosition'])
    url = server + 'indexFiles' 
    indexFilesFields = {
        'fileName': headers[0],
        'dataNodeIp': dataNode_ip,
        'blockPosition': int(headers[2]),
        'totalBlocks': int(headers[3])
    }
    
    response = requests.post(url, json=indexFilesFields)

    if response.status_code == 200:
        print("Message sent successfully to Server")
    else:
        print("Failed to send message to Server")

def consume_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_ip,rabbitmq_port))
    channel = connection.channel()
    channel.queue_declare(queue='queue')
    channel.basic_consume(queue='queue', on_message_callback=callback, auto_ack=True)
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
            if str(block["fileName"]) == str(targetBlock['fileName']) and int(block["blockPosition"]) == int(targetBlock['blockPosition']):
                client_socket.send(block["blockContent"])
                client_socket.close()
                break


def main():
    server_thread_obj = threading.Thread(target=server_thread, args=(dataNode_ip, 80))
    server_thread_obj.start()

    consume_messages()

if __name__ == "__main__":
    main()