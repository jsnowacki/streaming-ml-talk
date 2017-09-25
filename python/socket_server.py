import socket
import threading


class ThreadedServer(object):
    def __init__(self, host, port, deque):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.deque = deque

    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target=self.listen_to_client, args=(client, address)).start()

    def listen_to_client(self, client, address):
        size = 4096
        while True:
            try:
                data = client.recv(size).decode()
                if data:
                    self.deque.extend(data.strip().split('\n'))
                    # print("data ({}, {}): {}".format(len(data), len(data.strip().split('\n')), data))
                else:
                    raise socket.error('Client disconnected')
            except:
                client.close()


if __name__ == "__main__":
    while True:
        port_num = input("Port? ")
        try:
            port_num = int(port_num)
            break
        except ValueError:
            pass

    ThreadedServer('', port_num, None).listen()