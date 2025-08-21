import asyncio

from config import PROXY_HOST, PROXY_PORT
from proxy_server import run_server


def main() -> None:
    asyncio.run(run_server(PROXY_HOST, PROXY_PORT))


if __name__ == '__main__':
    main()


