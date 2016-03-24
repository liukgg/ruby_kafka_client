module RubyKafkaClient

  class Producer

    ##
    # ==== Description
    #
    # ==== Example
    # brokers = ["127.0.0.1:9092", "127.0.0.1:9092"]
    # client_id = "test_client"
    # RubyKafkaClient::Producer.new(brokers, client_id)
    ##
    def initialize(brokers, client_id)
      @brokers   = brokers
      @client_id = client_id

      @producer = ::Poseidon::Producer.new(@brokers, @client_id)
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
        ::Poseidon::MessageToSend.new(topic, msg.to_s)
      ]

      @producer.send_messages(poseidon_messages)
    end
  end

end
