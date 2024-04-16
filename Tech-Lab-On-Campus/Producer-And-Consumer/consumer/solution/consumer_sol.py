from consumer_interface import mqConsumerInterface
import pika

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # save parameters
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        # call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        self.con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name)
        self.channel.queue_bind(
            exchange=self.exchange_name, 
            queue=self.queue_name, 
            routing_key=self.binding_key
        )

        self.exchange = self.channel.exchange_declare(
            exchange=self.exchange_name, 
            exchange_type='direct'
        )
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=self.binding_key,
            body='Message'
        )

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            queue=self.queue_name, 
            self.on_message_callback, 
            auto_ack=False
        )
    
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        channel.basic_ack(method_frame.delivery_tag, False)
        print('Recieved Message: ', body)

    def startConsuming(self) -> None:
        print('[*] Waiting for messages. To exit press CTRL+C')
        self.channel.startConsuming()
    
    def __del__(self) -> None:
        print('Closing RMQ connection on destruction')
        self.channel.close()
        self.connection.close()


