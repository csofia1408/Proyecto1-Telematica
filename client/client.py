import pika
import os
import zlib
import socket
import requests
import json
import shutil

server = input("Enter server url: ")

resultFile = []

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='queue')

def add_header(data, filename, total_file_size, fragment_sequence, num_fragments):
    header = f"{filename}\n{total_file_size}\n{fragment_sequence}\n{num_fragments}\n".encode()
    return header + data

def fragment_and_compress(input_file, output_directory, block_size):
    block_size_bytes = block_size * 1024 

    with open("files/" + input_file, 'rb') as f:
        data = f.read()

    num_blocks = len(data) // block_size_bytes
    if len(data) % block_size_bytes != 0:
        num_blocks += 1

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    fragment_files = []
    for i in range(num_blocks):
        start_index = i * block_size_bytes
        end_index = min((i + 1) * block_size_bytes, len(data))
        block_data = data[start_index:end_index]

        filename = os.path.basename(input_file)
        total_file_size = len(data)
        fragment_sequence = i + 1

        compressed_data = zlib.compress(block_data, level=9)

        compressed_data_with_header = add_header(compressed_data, filename, total_file_size, fragment_sequence, num_blocks)

        output_file = os.path.join(output_directory, f"{os.path.splitext(filename)[0]}.{i}.bin")
        with open(output_file, 'wb') as f:
            f.write(compressed_data_with_header)

        fragment_files.append(output_file)

    print(f"File fragmented and compressed into {num_blocks} blocks.")
    return fragment_files

def reassemble_blocks(blocks, output_file):
    with open(output_file, 'wb') as f:
        for block in blocks:
            data = zlib.decompress(block)
            f.write(data)

    print("File reassembled.")

def list_files():
    files = []
    path = "files/"

    for file in os.listdir(path):
        files.append(file)
    
    return files

def validate_file(file, list_files):
    file = file.lower()
    for f in list_files:
        if file == f.lower():
            return f
    return False

while True:
    print("\nSelect a number to navigate through the menu.")
    print("1. List files in the system")
    print("2. Upload file")
    print("3. Download file\n")

    option = input("Option: ")

    if option == "1":
        print("List of files.")
        files = list_files()

        if len(files) == 0:
            print("There are no files yet.")

        for idx, file in enumerate(files):
            print(f"{idx+1}. {file}")
        input("\nPress any key to go back to the menu.")

    elif option == "2":
        files = list_files()

        if len(files) == 0:
            print("No files to upload.\n")
            continue

        input_file = input("Enter the name of the file to upload:")         

        original_file = validate_file(input_file, files)

        if original_file == False:
            print("The file does not exist.")
            continue

        output_directory = "Blocks"
        block_size = 4

        fragment_files = fragment_and_compress(input_file, output_directory, block_size)

        for _ in range(2):
            for fragment_file in fragment_files:
                with open(fragment_file, 'rb') as fragment:
                        fileName = fragment.readline().decode('utf-8').strip() + ","
                        size = fragment.readline().decode('utf-8').strip() + ","
                        currentBlock = fragment.readline().decode('utf-8').strip() + ","
                        totalBlocks = fragment.readline().decode('utf-8').strip()

                        compressed_data = fragment.read()

                        properties = pika.BasicProperties(
                            content_type = fileName + size  + currentBlock + totalBlocks
                        )
                        channel.basic_publish(exchange="", routing_key='queue', body=compressed_data, properties=properties)
        shutil.rmtree('Blocks')
    elif option == "3":
        file = input("Enter the name of the file to download:")

        url = server + 'indexFiles'
        params = {
            'fileName': file
        }
        response = requests.get(url, params=params)

        if response.status_code == 200:
            responseContent = json.loads(response.text)

            if len(responseContent['files']) > 0:
                for fragment in responseContent['files']:
                    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client_socket.connect(('127.0.0.1', int(fragment['dataNodeIp'])))
                    message = {
                        'fileName': fragment['fileName'], 
                        'blockPosition': fragment['blockPosition'],
                    }

                    client_socket.send(json.dumps(message).encode())
                    response = client_socket.recv(10000)

                    resultFile.append(response)
                    
                    client_socket.close()
                extension = responseContent['files'][0]["fileName"]
                reassemble_blocks(resultFile, 'files/' + str(extension))
            else:
                print("The file is not available to download.")
        else: 
            print("Failed to send message to Server")

        input("\nPress enter to go back to the menu.")
    else:
        print("Invalid option.")

