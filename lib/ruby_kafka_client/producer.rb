module RubyKafkaClient

  class Producer

    def initialize(borkers, client_id)
      @borker    = borkers
      @client_id = client_id

      @producer = ::Poseidon::Producer.new(@borker, @client_id)
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
