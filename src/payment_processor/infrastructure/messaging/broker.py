from faststream.rabbit import Channel, RabbitBroker

from payment_processor.core.config import settings

broker = RabbitBroker(settings.rabbitmq_url, default_channel=Channel(publisher_confirms=True, on_return_raises=True))
