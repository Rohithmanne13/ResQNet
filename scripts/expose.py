"""Make the minikube NodePorts reachable from Windows (WSL2): forwards localhost:<port> -> <minikube-ip>:<port>.

The minikube node IP (e.g. 192.168.49.2) lives on Docker's network inside WSL, which Windows cannot route to;
WSL does forward localhost ports to Windows. Forwarding to the NodePorts (not to single pods, as
`kubectl port-forward` does) keeps working while the demo kills and recreates pods.

  python3 scripts/expose.py            # then open http://localhost:30080/docs in the Windows browser
  python3 scripts/expose.py 192.168.49.2 30080 30081 30672
"""
import asyncio
import subprocess
import sys

PORTS = [30080, 30081, 30672]   # API, coordinator, RabbitMQ UI


async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


def handler(host, port):
    async def handle(client_r, client_w):
        try:
            up_r, up_w = await asyncio.open_connection(host, port)
        except OSError:
            client_w.close()
            return
        await asyncio.gather(pipe(client_r, up_w), pipe(up_r, client_w))
    return handle


async def main():
    args = sys.argv[1:]
    host = args.pop(0) if args and not args[0].isdigit() else \
        subprocess.run(["minikube", "ip"], capture_output=True, text=True, check=True).stdout.strip()
    ports = [int(p) for p in args] or PORTS
    servers = [await asyncio.start_server(handler(host, p), "127.0.0.1", p) for p in ports]
    for p in ports:
        print(f"http://localhost:{p}  ->  {host}:{p}", flush=True)
    print("Ctrl+C to stop", flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
