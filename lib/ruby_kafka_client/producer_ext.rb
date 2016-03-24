# encoding: utf-8
module RubyKafkaClient
  module ProducerExt

    extend ActiveSupport::Concern

    self.included do
      producer_options borker: Settings.kafka.borkers.split,
        client_id: Settings.kafka.client_id
    end

    module ClassMethods

      # 设置配置信息
      def producer_options options = {}
        options.assert_valid_keys(:borker, :client_id)

        @borker, @client_id = options[:borker], options[:client_id]
      end

      # 设置producer
      def producer
        Poseidon::Producer.new(@borker, @client_id)
      end

      ##
      # ==== Description
      # 指定partition的producer
      #
      # ==== TIPS
      # 对于需要将指定的key推送到同一个partition的业务场景，需要用该方法。
      #
      # ==== Example
      #  http://www.rubydoc.info/github/bpot/poseidon/Poseidon/Producer
      # 
      # You may also specify a custom partitioning scheme for messages by passing a Proc (or any object that responds to #call) to the Producer.
      # The proc must return a Fixnum >= 0 and less-than partition_count.
      # 
      # my_partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key) % partition_count }
      # producer = Producer.new(["broker1:port1", "broker2:port1"], "my_client_id",
      #                         :type => :sync, :partitioner => my_partitioner)
      ##
      def partition_producer
        my_partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key.to_s) % partition_count }
        Poseidon::Producer.new(@borker, @client_id, :partitioner => my_partitioner)
      end

      ##
      # ==== Description
      # 推送kafka消息
      #
      # ==== Parameters
      # msg:   要推送的kafka消息内容, String 类型
      # topic: 推送的消息对应的topic名称, String 类型
      #
      # ==== Returns
      # true || false
      ##
      def send_kafka_messages msg, topic
        poseidon_messages = [
          Poseidon::MessageToSend.new(topic, msg.to_s)
        ]

        producer.send_messages(poseidon_messages)
      end

      ##
      # ==== Description
      # 推送消息时，指定key，相同key的消息将会推送到同一个partition
      #
      # ==== NOTE
      #   Poseidon::MessageToSend.new(topic, msg.to_s, key.to_s) 中,
      #  key 必须传入字符串，否则可能无法生效
      #
      # ==== Returns
      #  true | false
      #
      # ==== Example
      ##
      def send_kafka_message_with_key(topic, msg, key)
        poseidon_messages = [
          Poseidon::MessageToSend.new(topic, msg.to_s, key.to_s)
        ]

        partition_producer.send_messages(poseidon_messages)
      end

    end
  end
end
