# encoding: utf-8

##
# ==== Description
# 该模块用于说明如何消费kafka消息
#
# ==== 如何新增kafka消费者组
# 1,在app/kafkas/consumers/ 目录下新增一个文件，如：
#   app/kafkas/consumers/example.rb
#
# 2,该文件中需要自定义一个消费者类,类名需要对应文件名
#   （1）消费者类必须定义在 Consumers 这个命名空间中;
#   （2）添加kafka消费相关代码： include ::Kafka::ConsumerConcern;
#   （3）需要实现类方法 run.
#
# 3,在run方法中, 通过new方法，传入 topic 创建消费者。
#
# 4,在run方法中，通过新增消费者的 handle_messages 方法，
#   请在block中处理业务逻辑。如果在处理业务逻辑的过程中出现异常，
#   则会导致进程挂掉，god会重启消费进程。
#
# ==== 如何使用god进行部署和监控
# 1,消费者进程通过god启动和监控, 实际上以rake形式启动消费者;
#
# 2,需要在god的配置文件中，添加进入该消费者(取消费者的文件名)：
#   god/xxxx.god文件, 加入到all_consumers中;
#
# 3,上线部署时需要重启god;
#   god terminate
#   (RAILS_ENV=development/production) god start -c god/xxxx.god
#   当不声明RAILS_ENV时，将默认以 production 环境启动。
#
# 4,目前暂时不支持以god restart 方式来重启kafka消费者进程;
#   可以通过文件 "/tmp/xxxx-kafka-#{idx}.pid"
#   来找到对应的kafka消费者进程的pid,
#   (其中 idx 表示该进程在 all_consumers 这个数组中的index).
#   然后杀死该进程即可实现重启;
#   因为god监控到该进程死掉之后，会再度将其启动。
#
# ==== 日志记录
# 1,默认log文件： log/kafka_messages_consumers_example_ruby.development.log
#   可以通过 logger.info 在该文件中记录日志.
# 2,通过rake启动消费进程时，日志文件：
#   log/kafka_consumer.development.log
# 3,通过god启动时，god相关日志文件：
#   "/tmp/god.rake.kafka.consumer.example.log"
#
# ==== 开发和测试TIPS
# 1,控制台启动方式： Consumers::Example.run
#
# 2,推送消息
# Consumers::Example.send_kafka_messages_to_test_topic
##

module Consumers
  class Example

    # Kafka消费机制
    include ::RubyKafkaClient::ConsumerExt

    TOPIC = "test_tp"

    class << self

      # 所有消费者都需要实现run方法
      def run
        cs = self.new(TOPIC)

        msg_count = 0

        cs.handle_messages do |partition, message|
          puts "[#{Time.now}] Fetched '#{message.value}' at #{message.offset} from #{partition}"
          msg_count += 1
          puts "pid: #{Process.pid}, message_count: #{msg_count}"
          # raise 'testaaaaaa' if rand(3) == 1 # 测试异常情况
          # raise "msg_count more than 10 !!!" if msg_count > 10
          # 处理业务逻辑
        end
      end

      def send_kafka_messages_to_test_topic
        messages = [
          Poseidon::MessageToSend.new(
            "test_tp", "time: #{Time.now.to_s(:db)}, test message")
        ]

        producer = Poseidon::Producer.new(
          ["127.0.0.1:9092","127.0.0.1:9092"],
          "test_client"
        )

        producer.send_messages(messages)
      end

    end

  end
end
