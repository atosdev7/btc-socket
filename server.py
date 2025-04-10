import socket
import threading
from client_handler import handle_client

def start_server(host='0.0.0.0', port=9090):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server listening on {host}:{port}")

    while True:
        conn, addr = server_socket.accept()
        print(f"Connection established with {addr}")
        client_thread = threading.Thread(target=handle_client, args=(conn,))
        client_thread.start()

if __name__ == "__main__":
    start_server()