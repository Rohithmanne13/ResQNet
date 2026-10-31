"""RabbitMQ topology.

  erms.requests (topic) --request.#--> matching.requests  (consumed by matching-service)
  erms.retry    (topic) --#----------> matching.retry     (TTL; dead-letters back to erms.requests)

A request that cannot be matched yet is published to erms.retry; it waits
RETRY_DELAY_MS in matching.retry and then dead-letters back onto the main queue.
"""
import asyncio
import logging
import os

import aio_pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE = "erms.requests"
RETRY_EXCHANGE = "erms.retry"
QUEUE = "matching.requests"
RETRY_QUEUE = "matching.retry"
RK_CREATED = "request.created"
RK_REQUEUED = "request.requeued"
RK_RETRY = "request.retry"

log = logging.getLogger("messaging")


async def maintain_connection(setup, on_disconnect, url: str = RABBITMQ_URL, delay: float = 3.0) -> None:
    """Connect, run setup(channel) (declare topology, start consumers), and when the
    connection drops (broker restart, network) reconnect and run setup again.

    We do this explicitly instead of relying on aio-pika's robust connection, whose
    automatic restore did not reliably re-declare queues/consumers after a broker
    restart that lost its state.
    """
    while True:
        conn = None
        try:
            conn = await aio_pika.connect(url)
            channel = await conn.channel(publisher_confirms=True)
            await setup(channel)
            log.info("connected to RabbitMQ; topology declared")
            while not conn.is_closed and not channel.is_closed:
                await asyncio.sleep(1)
            log.warning("RabbitMQ connection lost; reconnecting")
        except asyncio.CancelledError:
            if conn:
                await conn.close()
            raise
        except Exception as e:
            log.warning("RabbitMQ unavailable (%r); retrying in %.0fs", e, delay)
        on_disconnect()
        if conn and not conn.is_closed:
            try:
                await conn.close()
            except Exception:
                pass
        await asyncio.sleep(delay)


async def declare_main(channel) -> tuple:
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue(QUEUE, durable=True)
    await queue.bind(exchange, "request.#")
    return exchange, queue


async def declare_retry(channel, retry_delay_ms: int) -> aio_pika.abc.AbstractExchange:
    retry_exchange = await channel.declare_exchange(RETRY_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    retry_queue = await channel.declare_queue(
        RETRY_QUEUE,
        durable=True,
        arguments={"x-message-ttl": retry_delay_ms, "x-dead-letter-exchange": EXCHANGE},
    )
    await retry_queue.bind(retry_exchange, "#")
    return retry_exchange


def make_message(body: bytes) -> aio_pika.Message:
    return aio_pika.Message(body, content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
