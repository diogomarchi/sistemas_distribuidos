#Cliente
import socket
HOST = 'localhost'
PORT = 5000
tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
dest = (HOST, PORT)
tcp.connect(dest)
print('Para sair use CTRL+X\n')
msg = input()
while msg != '\x18':
    tcp.send(bytes(msg, 'utf-8'))
    msg = input()
tcp.close()