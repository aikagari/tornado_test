import asyncio
from tornado import websocket, web, ioloop
import aioredis
import os
import time

status_source_dict = {'\x01': 'IDLE', '\x02': 'ACTIVE', '\x03': 'RECHARGE'} 
connections = []
last_msg = {}

class BaseSocket(websocket.WebSocketHandler):
    async def open(self):
        connections.append(self)
        self.conn = await self.setup_redis()

    def on_message(self, message):
        pass

    def on_close(self):
        connections.remove(self)

    def check_origin(self):
        return True

    async def setup_redis(self):
        redis_url = os.environ.get('REDIS_HOST', 'localhost')
        conn =  await aioredis.create_redis(f'redis://{redis_url}')
        return conn


class ListenerApp(BaseSocket):
    def on_message(self, message):
        msg_to_resp = self.parse_message(message)
        self.write_message(msg_to_resp)

    def parse_message(self, message):
        msg_to_resp = ''
        try:
            message_list = [chr(int(x, 16)) for x in message.split(' ')]
            header = message_list[0]
            assert header == '\x01'
            
            num_message = ' '.join(str(ord(i)) for i in message_list[1:3])
            id_source = ''.join(str(ord(i)) for i in message_list[3:11])
            status_source = message_list[11]
            assert status_source in status_source_dict

            numfields = ord(message_list[12])

            message_data = {}
            for i in range(0, numfields):
                key_seq = l[12*i:12*i+8]
                value_seq =l[12*i+8:12*i+12]
                key_data = ''.join(str(ord(i)) for i in key_seq)
                value_data = ''.join(str(ord(i)) for i in value_seq)
                message_data[key_data] = value_data

            message_to_check = message[:-1]
            get_xor_hash(message_to_check)
            
            assert xor_data == message[-1]

            msg_result = '\r\n'.join("{} | {}".format(item[0], item[1]) for item in  message_data.items())
            
            last_msg[id_source] = {'time': int(round(time.time() * 1000)), 'num_message': num_message, 'status_source': status_source}
            self.conn.publish('messages', msg_result)
            num_to_resp = ' '.join(message_list[1:3])
            msg_to_resp = f'\x11 {num_to_resp}'
        except Exception:
            msg_to_resp = '\x12 \x00 \x00'
        
        xor_hash_resp = get_xor_hash(msg_to_resp)
        return f'{msg_to_resp} {xor_hash_resp}'


class ResponseApp(BaseSocket):
    async def open(self):
        await super().open()
        channel = await self.conn.subscribe('messages')
        self.channel = channel[0]
        current_time = int(round(time.time() * 1000))

        for key, value in last_msg.items():
            last_time = current_time - value['time']
            str_to_response = f"[{key}] {value['num_message']} | {status_source_dict[value['status_source']]} | {last_time}\r\n"
            await self.write_message(str_to_response)
        asyncio.ensure_future(self.consumer())
        
    async def consumer(self):
        while await self.channel.wait_message():
            message = await self.channel.get(encoding='utf-8')
            for connection in connections:
                await connection.write_message(message)

def get_xor_hash(message):
    xor_data = 0
    for item in message:
        xor_data ^= ord(item) 
    return hash(xor_data)

if __name__ == '__main__':
    app_response = web.Application([
        (r'/', ResponseApp)
    ])
    app_listener = web.Application([
        (r'/', ListenerApp)
    ])
    app_listener.listen(8888)
    app_response.listen(8889)
    loop = ioloop.IOLoop.current()
    loop.start()
