from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, RabbitPublisher
import asyncio

broker = RabbitBroker('amqp://admin:Emperor011@192.168.20.243:5672')
app = FastStream(broker)

queue = RabbitQueue('test_queue_1', durable=True)

@broker.subscriber(queue, 'test_key')

async def read_msg(msg):
    print(f"Message received: {msg}")
    