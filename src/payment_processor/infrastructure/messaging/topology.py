from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE_NAME = "payments.exchange"

PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_RETRY_1_QUEUE_NAME = "payments.retry.1"
PAYMENTS_RETRY_2_QUEUE_NAME = "payments.retry.2"
PAYMENTS_DLQ_QUEUE_NAME = "payments.dlq"

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, durable=True, type=ExchangeType.DIRECT)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW_QUEUE_NAME,
    routing_key=PAYMENTS_NEW_QUEUE_NAME,
    durable=True,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
        "x-dead-letter-routing-key": PAYMENTS_DLQ_QUEUE_NAME,
    },
)
PAYMENTS_RETRY_1_QUEUE = RabbitQueue(
    PAYMENTS_RETRY_1_QUEUE_NAME,
    routing_key=PAYMENTS_RETRY_1_QUEUE_NAME,
    durable=True,
    arguments={
        "x-message-ttl": 1_000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
        "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE_NAME,
    },
)
PAYMENTS_RETRY_2_QUEUE = RabbitQueue(
    PAYMENTS_RETRY_2_QUEUE_NAME,
    routing_key=PAYMENTS_RETRY_2_QUEUE_NAME,
    durable=True,
    arguments={
        "x-message-ttl": 2_000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
        "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE_NAME,
    },
)
PAYMENTS_DLQ_QUEUE = RabbitQueue(
    PAYMENTS_DLQ_QUEUE_NAME,
    routing_key=PAYMENTS_DLQ_QUEUE_NAME,
    durable=True,
)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(exchange=PAYMENTS_EXCHANGE)

    new_queue = await broker.declare_queue(queue=PAYMENTS_NEW_QUEUE)
    retry_1_queue = await broker.declare_queue(queue=PAYMENTS_RETRY_1_QUEUE)
    retry_2_queue = await broker.declare_queue(queue=PAYMENTS_RETRY_2_QUEUE)
    dlq_queue = await broker.declare_queue(queue=PAYMENTS_DLQ_QUEUE)

    await new_queue.bind(exchange=exchange)
    await retry_1_queue.bind(exchange=exchange)
    await retry_2_queue.bind(exchange=exchange)
    await dlq_queue.bind(exchange=exchange)
