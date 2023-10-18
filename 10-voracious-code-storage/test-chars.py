import socket

def main():
    for v in range(0, 0x100):
        # print(f"Testing {v} {chr(v)}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("vcs.protohackers.com", 30307))
            
            recv(s, 6)
            s.sendall(b"PUT /a 1\n" + bytes([v]))
            if b"ERR" in recv(s, 5):
                print(f"FAIL {v}")

def recv(s, i):
    data = bytearray()
    while len(data) < i:
        data.append(s.recv(1)[0])
    return data

if __name__ == "__main__":
    main()
