import asyncio
import struct
from typing import Tuple

from config import PG_HOST, PG_PORT, SSL_REQUEST_CODE, DEBUG_LOG_QUERIES
from sql_rewriter import rewrite_schema_table


async def _forward_server_to_client(server_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
    buffer = bytearray()
    try:
        while True:
            data = await server_reader.read(65536)
            if not data:
                break
            buffer.extend(data)

            while True:
                if len(buffer) < 5:
                    break
                msg_type = chr(buffer[0])
                msg_len = struct.unpack('!I', buffer[1:5])[0]
                total_len = 1 + msg_len
                if len(buffer) < total_len:
                    break
                body = bytes(buffer[5:total_len])

                if msg_type == 'R' and len(body) >= 4:
                    auth_code = struct.unpack('!I', body[:4])[0]
                    if auth_code == 0:
                        print('Auth: AuthenticationOk')
                    elif auth_code == 3:
                        print('Auth: CleartextPassword requisitado')
                    elif auth_code == 5:
                        print('Auth: MD5Password requisitado')
                    elif auth_code == 10:
                        print('Auth: SASL/SCRAM requisitado')
                    elif auth_code == 11:
                        print('Auth: SASLContinue')
                    elif auth_code == 12:
                        print('Auth: SASLFinal')
                    else:
                        print(f'Auth: código {auth_code}')
                elif msg_type == 'E':
                    try:
                        parts = body.split(b'\x00')
                        fields = {}
                        for part in parts:
                            if not part:
                                continue
                            k = chr(part[0])
                            v = part[1:].decode('utf-8', errors='replace')
                            fields[k] = v
                        sev = fields.get('S', 'error')
                        msg = fields.get('M', '')
                        code = fields.get('C', '')
                        print(f'Erro do servidor [{sev} {code}]: {msg}')
                    except Exception:
                        print('Erro do servidor (E) recebido')

                try:
                    client_writer.write(buffer[:total_len])
                    await client_writer.drain()
                except (ConnectionResetError, BrokenPipeError):
                    return
                del buffer[:total_len]
    except Exception as e:
        print(f'Erro no fluxo servidor->cliente: {e}')


async def _forward_client_to_server(client_reader: asyncio.StreamReader, server_writer: asyncio.StreamWriter, client_writer: asyncio.StreamWriter) -> None:
    buffer = bytearray()
    startup_phase = True
    try:
        while True:
            chunk = await client_reader.read(65536)
            if not chunk:
                break
            buffer.extend(chunk)

            while True:
                if startup_phase:
                    if len(buffer) < 8:
                        break
                    msg_len = struct.unpack('!I', buffer[0:4])[0]
                    if len(buffer) < msg_len:
                        break
                    code = struct.unpack('!I', buffer[4:8])[0]
                    if msg_len == 8 and code == SSL_REQUEST_CODE:
                        client_writer.write(b'N')
                        await client_writer.drain()
                        del buffer[:8]
                        continue
                    else:
                        try:
                            server_writer.write(buffer[:msg_len])
                            await server_writer.drain()
                        except (ConnectionResetError, BrokenPipeError):
                            return
                        del buffer[:msg_len]
                        if code != SSL_REQUEST_CODE:
                            startup_phase = False
                        continue

                if len(buffer) < 5:
                    break
                msg_type = chr(buffer[0])
                msg_len = struct.unpack('!I', buffer[1:5])[0]
                total_len = 1 + msg_len
                if len(buffer) < total_len:
                    break
                body = bytes(buffer[5:total_len])

                if msg_type == 'Q':
                    try:
                        nul_idx = body.find(b'\x00')
                        if nul_idx == -1:
                            try:
                                server_writer.write(buffer[:total_len])
                                await server_writer.drain()
                            except (ConnectionResetError, BrokenPipeError):
                                return
                        else:
                            query_bytes = body[:nul_idx]
                            tail = body[nul_idx + 1:]
                            query = query_bytes.decode('utf-8', errors='replace')
                            if DEBUG_LOG_QUERIES:
                                print(f'Query original (Q): {query}')
                            new_query = rewrite_schema_table(query)
                            if new_query != query:
                                print(f'Rewrite Q: {query} -> {new_query}')
                            new_query_bytes = new_query.encode('utf-8')
                            new_body = new_query_bytes + b'\x00' + tail
                            new_len = 4 + len(new_body)
                            try:
                                server_writer.write(bytes([ord('Q')]) + struct.pack('!I', new_len) + new_body)
                                await server_writer.drain()
                            except (ConnectionResetError, BrokenPipeError):
                                return
                    except Exception as e:
                        print(f'Erro reescrevendo Q: {e}')
                        try:
                            server_writer.write(buffer[:total_len])
                            await server_writer.drain()
                        except (ConnectionResetError, BrokenPipeError):
                            return
                elif msg_type == 'P':
                    try:
                        nul1 = body.find(b'\x00')
                        if nul1 == -1:
                            try:
                                server_writer.write(buffer[:total_len])
                                await server_writer.drain()
                            except (ConnectionResetError, BrokenPipeError):
                                return
                        else:
                            name = body[:nul1 + 1]
                            rest = body[nul1 + 1:]
                            nul2 = rest.find(b'\x00')
                            if nul2 == -1:
                                try:
                                    server_writer.write(buffer[:total_len])
                                    await server_writer.drain()
                                except (ConnectionResetError, BrokenPipeError):
                                    return
                            else:
                                query_bytes = rest[:nul2]
                                tail = rest[nul2 + 1:]
                                query = query_bytes.decode('utf-8', errors='replace')
                                if DEBUG_LOG_QUERIES:
                                    print(f'Query original (P): {query}')
                                new_query = rewrite_schema_table(query)
                                if new_query != query:
                                    print(f'Rewrite P: {query} -> {new_query}')
                                new_query_bytes = new_query.encode('utf-8')
                                new_body = name + new_query_bytes + b'\x00' + tail
                                new_len = 4 + len(new_body)
                                try:
                                    server_writer.write(bytes([ord('P')]) + struct.pack('!I', new_len) + new_body)
                                    await server_writer.drain()
                                except (ConnectionResetError, BrokenPipeError):
                                    return
                    except Exception as e:
                        print(f'Erro reescrevendo P: {e}')
                        try:
                            server_writer.write(buffer[:total_len])
                            await server_writer.drain()
                        except (ConnectionResetError, BrokenPipeError):
                            return
                else:
                    try:
                        server_writer.write(buffer[:total_len])
                        await server_writer.drain()
                    except (ConnectionResetError, BrokenPipeError):
                        return

                del buffer[:total_len]
    except Exception as e:
        print(f'Erro no fluxo cliente->servidor: {e}')


async def handle_client(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
    peer = client_writer.get_extra_info('peername')
    print(f'Cliente conectado: {peer}')
    try:
        server_reader, server_writer = await asyncio.open_connection(PG_HOST, PG_PORT)
        print(f'Conectado ao PostgreSQL real em {PG_HOST}:{PG_PORT}')

        t1 = asyncio.create_task(_forward_client_to_server(client_reader, server_writer, client_writer))
        t2 = asyncio.create_task(_forward_server_to_client(server_reader, client_writer))
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            try:
                await task
            except Exception:
                pass
    except Exception as e:
        print(f'Erro ao lidar com cliente {peer}: {e}')
    finally:
        try:
            client_writer.close()
            await client_writer.drain()
        except Exception:
            pass


async def run_server(host: str, port: int) -> Tuple[str, int]:
    server = await asyncio.start_server(handle_client, host, port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f'Proxy rodando em {addrs} (ssl preferido será negado com N)')
    async with server:
        await server.serve_forever()


