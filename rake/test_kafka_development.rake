# encoding: utf-8

##
# ==== Descripton
# kafka消费的相关gem说明--poseidon_cluster
#
# 1,如果不进行额外设置，那么当一个topic长时间没有新消息时，
#   Poseidon::ConsumerGroup#fetch_loop 方法会报超时错误如下：
# "捕获到超时异常：当前时间：2015-12-10 15:51:14， #<Poseidon::Connection::ConnectionFailedError: Failed to connect to witgo:9092>"
#
# 解决方案： 
# 调用Poseidon::ConsumerGroup.new时，传入option参数: socket_timeout_ms: 100000
#
# 这里报错的具体位置在：
# lib/poseidon/connection.rb文件中， Poseidon::Connection#raise_connection_failed_error
#    def raise_connection_failed_error
#      raise ConnectionFailedError, "Failed to connect to #{@host}:#{@port}"
#    end
#
# 2, 调用Poseidon::ConsumerGroup.new时, 暂时不支持option参数： claim_timeout
#    关于这一点，已经发送邮件给poseidon_cluster的作者，详见邮件。
#    后续应该会修复该问题。
#
##
namespace :zhaoshang do
  namespace :kafka do

    # rake zhaoshang:kafka:test_kafka
    desc '开发环境测试kafka'
    task :test_kafka => [:environment] do
      test_kafka_in_development
    end

    # 测试消费消息
    task :test_consume_messages, [:topic] => [:environment] do |t, args|
      puts "开始建立 Poseidon::ConsumerGroup ..."

      topic = args.topic || "test_tp"
      stm   = 7 * 24 * 60 * 60 * 1000

      consumer = Poseidon::ConsumerGroup.new(
        "test_group_for_#{topic}",
        ["192.168.5.158:9092", "192.168.5.159:9092"],
        ["192.168.5.158:2181", "192.168.5.159:2181"],
        topic,
        socket_timeout_ms: stm
      )

      puts "开始消费消息。。。"
      consumer.fetch_loop do |partition, messages|
        messages.each do |m|
          puts "[#{Time.now}] Fetched '#{m.value}' at offset: #{m.offset} from partition: #{partition}"
        end
      end
    end

    def test_send_kafka_messages
      messages = [Poseidon::MessageToSend.new("test_tp", "time: #{Time.now.to_s}, 123test00")]
      producer = Poseidon::Producer.new(["192.168.5.158:9092","192.168.5.159:9092"], "zhaoshang_client")
      producer.send_messages(messages)
    end

    ##
    # ==== Descripton
    # 开发环境测试kafka
    #
    # 商城topic： hm_product_update
    #
    # 参考：
    # https://github.com/bpot/poseidon
    # https://github.com/bsm/poseidon_cluster
    ##
    def test_kafka_in_development
      return unless Rails.env.development?

      puts "开始建立 Poseidon::ConsumerGroup ..."

      stm = 7 * 24 * 60 * 60 * 1000
      consumer = Poseidon::ConsumerGroup.new(
        "test_group",
        ["192.168.5.158:9092", "192.168.5.159:9092"],
        ["192.168.5.158:2181", "192.168.5.159:2181"],
        "test_tp",
        socket_timeout_ms: stm
      )

      puts "监控并获取kafka消息..."

      begin
        consumer.fetch_loop do |partition, messages|
          messages.each do |m|
            puts "[#{Time.now}] Fetched '#{m.value}' at #{m.offset} from #{partition}"
          end
        end
      rescue Poseidon::Connection::ConnectionFailedError => ex
        puts "捕获到超时异常：当前时间：#{Time.now.to_s(:db)}， #{ex.inspect}"
        puts "重新建立链接。。。"
        retry
      end

      puts "退出监控。"
    end

  end
end
