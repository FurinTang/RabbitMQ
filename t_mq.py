# -*- coding: utf-8 -*-
# t_mq.py 【一行代码实现生产者】+ 规范路由键 + 三种交换机全覆盖 + 适配改造后的mq.py
from mq import RabbitMQServer

# ===================== 队列/交换机/路由键 常量【全部规范定义，生产标准】 =====================
# 队列名
QUEUE_BASE = "queue_base_direct"
QUEUE_DIRECT = "queue_exchange_direct"
QUEUE_TOPIC_USER = "queue_topic_user_info"
QUEUE_TOPIC_ORDER = "queue_topic_order_pay"
QUEUE_FANOUT_01 = "queue_fanout_all_01"
QUEUE_FANOUT_02 = "queue_fanout_all_02"

# 交换机名
EXCHANGE_DIRECT = "exchange_test_direct"
EXCHANGE_TOPIC = "exchange_test_topic"
EXCHANGE_FANOUT = "exchange_test_fanout"

# 路由键【严格规范，对应每种交换机特性，重点！】
RK_BASE = QUEUE_BASE                  # 默认直连-路由键=队列名(原生规则)
RK_DIRECT = "biz.order.pay_success"   # 直连交换机-精准业务路由键
RK_TOPIC_USER_BIND = "user.#"         # 主题交换机-绑定用户队列的通配路由键
RK_TOPIC_ORDER_BIND = "*.order"       # 主题交换机-绑定订单队列的通配路由键
RK_TOPIC_USER_ADD = "user.info.register"  # 主题交换机-发送用户消息的真实路由键
RK_TOPIC_ORDER_PAY = "trade.order.pay"    # 主题交换机-发送订单消息的真实路由键
RK_FANOUT = ""                        # 扇出交换机-路由键固定为空(官方标准，无视路由键)

# ===================== 通用消费回调 - 手动ACK + 日志清晰 + 完整打印路由键/交换机 =====================
def mq_callback(ch, method, properties, body):
    try:
        msg = body.decode("utf-8")
        print("-" * 70)
        print("✅ 【消费成功】")
        print(f"📡 交换机：{method.exchange if method.exchange else '默认空交换机(原生直连)'}")
        print(f"🔑 匹配路由键：{method.routing_key}")
        print(f"🗯️  消息内容：{msg}")
        print("-" * 70)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"❌ 消费失败：{str(e)}，消息重回队列")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# ===================== ✅核心：生产者【纯一行代码发送一条消息】无任何多余操作 =====================
def run_producer():
    print("=" * 70)
    print("🚀 生产者启动【一行代码发送所有消息】默认直连+Direct+Topic+Fanout 全覆盖")
    print("=" * 70)
    mq = RabbitMQServer()

    # ✅ 1. 默认原生直连模式 (无交换机) → 一行代码
    mq.send_message(queue_name=QUEUE_BASE,
                    message="默认直连 → 无交换机，路由键等于队列名，精准投递，一行代码搞定",
                    routing_key=RK_BASE)

    # ✅ 2. 直连交换机 Direct (精准匹配路由键) → 一行代码 【exchange+exchange_type+routing_key 必传】
    mq.send_message(queue_name=QUEUE_DIRECT,
                    message=f"直连交换机 → 仅[{RK_DIRECT}]路由键可匹配，精准一对一投递",
                    exchange=EXCHANGE_DIRECT,
                    exchange_type="direct",
                    routing_key=RK_DIRECT)

    # ✅ 3. 主题交换机 Topic (通配符匹配) → 一行代码 【核心体现通配规则，两条测试消息】
    mq.send_message(queue_name=QUEUE_TOPIC_USER,
                    message=f"主题交换机 → [{RK_TOPIC_USER_ADD}] 匹配绑定的 [{RK_TOPIC_USER_BIND}] 通配规则，用户注册消息",
                    exchange=EXCHANGE_TOPIC,
                    exchange_type="topic",
                    routing_key=RK_TOPIC_USER_ADD)
    mq.send_message(queue_name=QUEUE_TOPIC_ORDER,
                    message=f"主题交换机 → [{RK_TOPIC_ORDER_PAY}] 匹配绑定的 [{RK_TOPIC_ORDER_BIND}] 通配规则，订单支付消息",
                    exchange=EXCHANGE_TOPIC,
                    exchange_type="topic",
                    routing_key=RK_TOPIC_ORDER_PAY)

    # ✅ 4. 扇出交换机 Fanout (广播模式 无视路由键) → 一行代码 【路由键固定传空，官方标准】
    mq.send_message(queue_name=QUEUE_FANOUT_01,
                    message="扇出交换机 → 广播消息，所有绑定队列全收到，路由键无效(传空)，一行代码搞定",
                    exchange=EXCHANGE_FANOUT,
                    exchange_type="fanout",
                    routing_key=RK_FANOUT)
    mq.send_message(queue_name=QUEUE_FANOUT_02,
                    message="扇出交换机 → 广播消息，所有绑定队列全收到，路由键无效(传空)，一行代码搞定",
                    exchange=EXCHANGE_FANOUT,
                    exchange_type="fanout",
                    routing_key=RK_FANOUT)

    print("\n" + "=" * 70)
    print("✅ 全部消息发送完成！所有消息均【一行代码】发送，路由键规范填写！")
    print("=" * 70)
    mq.close()

# ===================== 消费者：监听所有队列 一键消费 无改动 =====================
def run_consumer():
    print("=" * 70)
    print("🚀 消费者启动：监听所有测试队列")
    print("=" * 70)
    mq = RabbitMQServer()
    mq.channel.basic_qos(prefetch_count=30)  # 限流防堆积，生产必备

    # 注册所有队列监听
    mq.channel.basic_consume(queue=QUEUE_BASE, on_message_callback=mq_callback, auto_ack=False)
    mq.channel.basic_consume(queue=QUEUE_DIRECT, on_message_callback=mq_callback, auto_ack=False)
    mq.channel.basic_consume(queue=QUEUE_TOPIC_USER, on_message_callback=mq_callback, auto_ack=False)
    mq.channel.basic_consume(queue=QUEUE_TOPIC_ORDER, on_message_callback=mq_callback, auto_ack=False)
    mq.channel.basic_consume(queue=QUEUE_FANOUT_01, on_message_callback=mq_callback, auto_ack=False)
    mq.channel.basic_consume(queue=QUEUE_FANOUT_02, on_message_callback=mq_callback, auto_ack=False)

    print("✅ 所有队列监听成功，等待消息 (按 Ctrl+C 优雅退出)")
    try:
        mq.channel.start_consuming()
    except KeyboardInterrupt:
        mq.channel.stop_consuming()
        mq.close()
        print("\n✅ 消费停止，RabbitMQ连接已优雅关闭")

# ===================== 运行入口 二选一 =====================
if __name__ == "__main__":
    # --- 第一步：先运行生产者发送消息 (打开此行，注释下面) ---
    # run_producer()

    # --- 第二步：再运行消费者消费消息 (注释上面，打开此行) ---
    run_consumer()