import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError



CONFIG = {
    "host" : "127.0.0.1",
    "port" : 5672,
    "username" : "guest",
    "password" : "guest",
    "virtual_host" : "/",
    "connection_attempts" : 3,     # 重连次数
    "retry_timeout" : 2,           # 重连间隔
    "heartbeat_timeout" : 60,
}




class RabbitMQServer:
    def __init__(self):
        self.config = CONFIG
        self.channel = None
        self.connection = None
        self._create_connection()


    def _create_connection(self):

        try:
            # 构建MQ凭证
            credentials = pika.PlainCredentials(self.config['username'], self.config['password'])
            # 构建连接参数
            parameters = pika.ConnectionParameters(
                host = self.config['host'],
                port = self.config['port'],
                virtual_host = self.config['virtual_host'],
                credentials = credentials,
                connection_attempts = self.config['connection_attempts'],
                retry_delay = self.config['retry_timeout'],
                heartbeat = self.config['heartbeat_timeout']
            )
            # 创建连接和信道
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            print("RabbitMQ 生产者连接成功！")
        except AMQPConnectionError as e:
            print(f"RabbitMQ 生产者连接失败：{str(e)}")
            raise e
        except Exception as e:
            print(f"RabbitMQ 生产者未知连接异常:{str(e)}")
            raise e

    # 重连
    def _reconnect(self):
        print("RabbitMQ连接断开，开始自动重连...")
        self.connection = None
        self.channel = None
        self._create_connection()

    # 声明队列
    def declare_queue(self, queue_name : str, durable : bool=True):
        try:
            self.channel.queue_declare(queue=queue_name, durable=durable, auto_delete=False, exclusive=False)   # auto_delete=False持久化队列，exclusive=False保证其它连接也可看见队列
            print(f"队列声明成功：{queue_name}  队列持久{durable}")
        except AMQPChannelError as e:
            print(f"队列声明失败：{str(e)},开始重试")
            self._reconnect()
            self.channel.queue_declare(queue=queue_name, durable=durable, auto_delete=False, exclusive=False)





    # 声明交换机
    def declare_exchange(self, exchange_name: str, exchange_type: str, durable: bool = True):
        try:
            self.channel.exchange_declare(exchange=exchange_name,
                                          exchange_type=exchange_type,
                                          durable=durable
                                          )
            print(f"交换机声明成功：{exchange_name}  类型={exchange_type}  持久={durable}")
        except AMQPChannelError as e:
            print(f"交换机声明失败：{str(e)},开始重试")
            self._reconnect()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=durable)





    # 队列绑定交换机
    def bind_queue(self, queue_name: str, exchange_name: str, routing_key: str = ""):
        try:
            # 先声明队列再绑定
            self.declare_queue(queue_name)
            self.channel.queue_bind(queue=queue_name, exchange=exchange_name)
            print(f"绑定成功：队列[{queue_name}] → 交换机[{exchange_name}]  路由键={routing_key}")
        except AMQPChannelError as e:
            print(f"队列绑定失败：{str(e)},开始重试")
            self._reconnect()
            self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)





    # 建立生产者
    def send_message(self, queue_name : str,
                     message : str,
                     exchange : str="",
                     exchange_type :str="",
                     routing_key : str = None,
                     durable_msg: bool = True):
        """
        Direct
        直连交换机：路由键必须完全精准匹配，一对一投递，企业最常用
        Topic
        主题交换机：支持通配符 * (匹配 1 个词)
        和  # (匹配 0 + 个词)，一对多灵活匹配，适用大部分业务场景
        Fanout
        扇出交换机：无视路由键，广播消息到所有绑定的队列，一对多强制投递，适用广播通知场景
        """
        try:
            # 1.判断交换机并声明队列
            if routing_key is None:
                routing_key = queue_name

            # 交换机非空，进行绑定
            if exchange.strip() != "":
                self.declare_exchange(exchange, exchange_type)      # 自动声明交换机
                self.bind_queue(queue_name, exchange, routing_key)  # 自动绑定队列+交换机

            # 交换机为空，则默认为直连
            else:
                self.declare_queue(queue_name)

            # 2.发送消息
            self.channel.basic_publish(exchange=exchange,
                                       routing_key=routing_key,
                                       body=message,
                                       properties=pika.BasicProperties(delivery_mode=2 if durable_msg else 1)
                                       )
            print(f"消息发送成功 -> 队列[{queue_name}]：{message}")

        except AMQPChannelError as e:
            print(f"消息发送失败：{str(e)}")
            self._reconnect()  # 连接/信道异常，重连后重试发送
            self.send_message(queue_name, message, exchange, routing_key, durable_msg)

        except Exception as e:
            print(f"消息发送未知异常：{str(e)}")
            raise e






    # 监听队列并消费
    def consume_message(self, queue_name : str, callback_func, auto_ack : bool=False):   # auto_ack : bool=False手动确认，callback_func业务函数
        try:
            # 1.声明队列
            self.declare_queue(queue_name)
            # 2.消费信息
            self.channel.basic_qos(prefetch_count=30)   # 配置消费规则，决定一个消费者一次消费多少条消息
            self.channel.basic_consume(queue=queue_name,
                                       on_message_callback=callback_func,
                                       auto_ack=auto_ack
                                       )
            print(f"开始监听队列 -> [{queue_name}]，等待消息...")
            self.channel.start_consuming()
        except AMQPChannelError as e:
            print(f"消费消息异常：{str(e)}")
            # 异常重连后继续消费
            self._reconnect()
            self.consume_message(queue_name, callback_func, auto_ack)
        except KeyboardInterrupt:
            # 手动终止程序时优雅退出
            self.channel.stop_consuming()
            self.close()
            print("消费者手动终止，优雅退出")
        except Exception as e:
            print(f"消费未知异常：{str(e)}")
            raise e





    # 关闭连接
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("RabbitMQ 连接已关闭")
        if self.channel and not self.channel.is_closed:
            self.channel.close()
            print("RabbitMQ 通道已关闭")


