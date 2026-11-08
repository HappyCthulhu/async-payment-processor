from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

NEW_PAYMENTS_Q_NAME = "payments.new"
PAYMENTS_EXCHANGE_NAME = "payments.exchange"

PAYMENT_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, durable=True, type=ExchangeType.DIRECT)
PAYMENT_NEW_QUEUE = RabbitQueue(NEW_PAYMENTS_Q_NAME, routing_key=NEW_PAYMENTS_Q_NAME, durable=True)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(exchange=PAYMENT_EXCHANGE)
    queue = await broker.declare_queue(queue=PAYMENT_NEW_QUEUE)
    await queue.bind(exchange=exchange)
