# encoding: utf-8

##
# ==== Description
# 参考：
# https://github.com/bpot/poseidon
# https://github.com/bsm/poseidon_cluster
# http://www.rubydoc.info/github/bsm/poseidon_cluster/Poseidon/ConsumerGroup
#
# ==== How to add a new consumer
# 1,新增消费者，需要在app/kafkas/consumers/ 目录下新增一个文件，
#   如： app/kafkas/consumers/example.rb;
#
# 2,该文件中需要自定义一个消费者类，如： ::Consumers::Example;
#   所有消费者类需要实现类方法 run,
#   在run方法中使用fetch_loop来监控kafka队列消息并且进行消费;
#   需要根据业务场景来决定是否使用选项commit: false, 使用该选项时，
#   需要在业务逻辑处理成功后，手动进行 commit.
#
# 3,消费者进程通过god启动和监控, 实际上以rake形式启动消费者;
#   需要在god的配置文件中，添加进入该消费者：
#   god/tao800_fire.god文件, 加入到all_consumers中;
#
#   NOTE:请注意，加入到 all_consumers 中的名称为 文件名，
#   启动时会将其转化为大写后，找其对应class, 如：
#   all_consumers = %w(example seller_online_offline)
#   则将会启动两个消费者进程，分别为： 
#   Consumers::Example.run, Consumers::SellerOnlineOffline.run
#
# 4,上线部署时需要重启god;
#   god terminate
#   (RAILS_ENV=development/production) god start -c god/tao800_fire.god
#   当不声明RAILS_ENV时，将默认以 production 环境启动。
#
# 5,目前暂时不支持以god restart 方式来重启kafka消费者进程;
#   可以通过文件 "/tmp/tao800_fire-kafka-#{consumer_name}.pid" 来找到对应的kafka消费者进程的pid;
#   然后杀死该进程即可实现重启;因为god监控到该进程死掉之后，会再度将其启动。
#
# 6,目前默认配置为如果一个消费者监控的topic在7天内都没有新的消息，那么会连接超时;
#   这时进程会挂掉，god监控会负责重新启动。
#   另外，如果在处理过程中发生了异常，那么进程也可能会挂掉，god会负责重新启动。
#
# ==== TODO LIST
# 1,commit: false的作用：不会设置offset增加1, 如果进程重启，消费者rebalance!,
#   那么会从没有commit的offset开始重新获取消息。
# 2,长时间（如10个小时）没有消息后，有一个新消息？
#   之前试验，没有出现问题。--需要进行观察。
# 3,目前god可以监控kafka消费者进程并且保证其在dead后被启动;
#   但是目前有2个问题，后续需要考虑解决：
#   （1）启动kafka消费进程时，由于有一个无限循环，导致会有2个进程存在;
#   （2）god stop命令对于kafka进程不好用，目前原因不详，
#        暂时需要直接用 kill 命令杀死进程。
# 4,后续根据需求，考虑支持更多参数配置。
# 5,目前为试用阶段，记录了很多日志，后续可以根据实际情况精简日志。
# 6,目前声明的实例变量较多，后续需要再整理下,去掉不必要的实例变量。
# 7,请考虑一下如何 添加check 脚本
# 8,可以提供一个 类似于 rake kafafka:generate 的命令，生成该文件
#   可以参考thor 和 rails_service (template方法)
#   https://github.com/erikhuda/thor/blob/f0c2166534e122636f5ce04d61885736ef605617/lib/thor/actions/file_manipulation.rb
# 9,关于线程安全，线程和进程的关系处理，需要进一步研究。
# 10,线程的退出是否都是Exception导致的？研究Thread#at_exit方法。
# 11,明确如何写单元测试并补充足够测试。
# 12,使用方式优化,类变量、配置方式等进一步考虑和优化。
##
module RubyKafkaClient

  # 一个消费者组内，消费者数量不能大于partitions数量
  # 不建议直接继承 Exception，建议继承 StandardError, 
  # 原因请看 http://stackoverflow.com/questions/10048173/why-is-it-bad-style-to-rescue-exception-e-in-ruby
  class ConsumerCountError < StandardError;end

  module ConsumerExt

    extend ActiveSupport::Concern

    BROKERS           = Settings.kafka.borkers.split
    ZOOKEEPERS        = Settings.kafka.zookeepers.split
    SOCKET_TIMEOUT_MS = 7 * 24 * 60 * 60 * 1000 # ms
    LATEST_OFFSET     = -1

    # 线上可能会启动多个god进程，意味着会启动多个kafka消费进程，而同一个group内的不同消费者
    # 不能同时访问同一个partition，所以默认只产生一个consumer.
    DEFAULT_CONSUMER_COUNT = 1

    attr_reader :topic, :group_name, :brokers, :zookeepers,
      :logger, :consumer_count, :consumers,
      :consumer, :partition_count

    ##
    # ==== Description
    # 获取消息并且进行处理，block中负责处理业务逻辑
    #
    # @api public
    ##
    def handle_messages(&block)
      logger.info "begin handle_messages."

      begin
        consumer_fetch_loop_and_handle(&block)
      rescue Poseidon::Errors::OffsetOutOfRange => offset_ex
        logger.info "====当前各个partition的offset为："
        partition_count.times{ |num| logger.info "partion-#{num}-offset: #{consumer.offset(num)}" }
        record_exception_log offset_ex

        # offset 重置;这里重置了zookeepers上面的offset，从最新的offset开始消费，
        # 但是没有重置consumer本身的实例变量consumers中每个consumer的offset;
        # consumer.instance_variable_get(:@consumers)
        # 所以，这里仍然抛出异常，该进程挂掉，然后god重启该进程。
        consumer.partitions.each { |p| consumer.commit(p.id, LATEST_OFFSET) }

        logger.info "offset 重置完毕;结果："
        partition_count.times{ |num| logger.info "partion-#{num}-offset: #{consumer.offset(num)}" }

        raise offset_ex
      end
    end

    def consumer_fetch_loop_and_handle(&block)
      logger.info "begin fetch_loop."

      consumer.fetch_loop do |partition, messages|
        logger.info "current_partition: #{partition}" if Rails.env.development?

        messages.each do |m|
          have_retried = false
          value_utf8   = m.value.force_encoding('utf-8')

          logger.info "Fetched '#{value_utf8}' at #{m.offset} from #{partition}"
          logger.info "Start handling this message..."

          begin
            block.call(partition, m)
            logger.info "Handling Succeeded! partition: #{partition}, offset: #{m.offset}"
          rescue StandardError => ex
            record_exception_log ex, partition: partition, offset: m.offset

            if have_retried
              raise ex
            else
              logger.info "Start to retry handling this message..."
              have_retried = true
              retry
            end
          rescue Exception => other_ex
            record_exception_log other_ex, partition: partition, offset: m.offset
            raise other_ex
          end
        end
      end
    end

    ##
    # ==== Description
    # 初始化一个新的消费者组
    #
    # ==== Parameters
    # brokers:    Kafka集群地址
    # zookeepers: Kafka调度系统Zookeepers集群地址
    # topic:      Kafka的topic名称
    # group_name: 消费者组的名称，同一个组内可以声明多个消费者，
    #             但是不能多于partition的数量,
    #             因为一个partition同一时间只能被同一个组内的一个消费者消费。
    # opts:
    #   consumer_count:    消费者组内的消费者数量
    #   socket_timeout_ms: 消费者超时时间，当超过该时间限制后都没有新消息时，
    #                      Poseidon会抛出超时异常。
    #
    ##
    def initialize(topic, options = {})
      @brokers    = options[:brokers] || BROKERS
      @zookeepers = options[:zookeepers] || ZOOKEEPERS
      @topic      = topic
      @group_name = self.class.name.gsub(/::/, '').underscore + '_ruby'
      options[:socket_timeout_ms] ||= SOCKET_TIMEOUT_MS

      init_logger
      init_consumers_and_count options

      logger.info "initializing done."
    end

    protected

    ##
    # ==== Description
    # 设置默认的日志文件路径
    ##
    def init_logger
      log_file = "#{Rails.root}/log/kafka_messages_" +
        group_name.underscore + "." + Rails.env + ".log"

      @logger = Logger.new log_file
      @logger.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.to_s(:db)}] #{msg}\n"
      end
    end

    ##
    # ==== Description
    # 设置消费者组内的consumers数量并创建consumers
    #
    # V1:
    # 默认情况下，设置消费者数量为partitions数量。
    #
    # V2:
    # V1有问题，由于线上有4个god进程，导致会启动4个消费进程，
    # 如果每个消费进程里的消费者数量都和partition数量相等，
    # 竞争失败的消费者claim会超时，等待超时。
    # 所以，改为默认只有1个消费者,但是支持自己合理设置多个消费者。
    #
    # ==== NOTE
    # 目前只支持一个consumer，线上会有多个进程。
    ##
    def init_consumers_and_count(options = {})
      logger.info "initializing consumers..."

      @consumer        = new_consumer_group(options)
      @partition_count = @consumer.partitions.count

      #@consumer_count = options.delete(:consumer_count) || DEFAULT_CONSUMER_COUNT

      #if @consumer_count.to_i <= 0 || @consumer_count > partition_count
        #raise ::Kafka::ConsumerCountError
      #else
        #@consumers = [cs]
        #(@consumer_count - 1).times { @consumers << new_consumer_group(options) }
      #end
    end

    ##
    # ==== Description
    # 为消费者组内的每个consumers创建一个线程，用于并发处理各个partitions的消息
    #
    # 关于线程里面发生异常时，如何退出整个进程，参考：
    # http://stackoverflow.com/questions/9095316/handling-exceptions-raised-in-a-ruby-thread
    ##
    def set_threads_for_consumers(&block)
      logger.info "setting threads..."

      threads = consumers.inject([]) do |result, consumer|
        thread = Thread.new do
          consumer.fetch_loop do |partition, messages|
            messages.each do |m|
              have_retried = false
              value_utf8   = m.value.force_encoding('utf-8')

              logger.info "Fetched '#{value_utf8}' at #{m.offset} from #{partition}"
              logger.info "Start handling this message..."

              begin
                block.call(partition, m)
                logger.info "Handling Succeeded! partition: #{partition}, offset: #{m.offset}"
              rescue StandardError => ex
                record_exception_log ex, partition: partition, offset: m.offset

                if have_retried
                  raise ex
                else
                  logger.info "Start to retry handling this message..."
                  have_retried = true
                  retry
                end
              rescue Exception => other_ex
                record_exception_log other_ex, partition: partition, offset: m.offset
                raise other_ex
              end
            end
          end
        end

        thread.abort_on_exception = true

        result << thread
      end

      logger.info "setting threads done."

      threads
    end

    ##
    # ==== Description
    # 发生异常时记录日志
    ##
    def record_exception_log(ex, info = {})
      logger.info "Handling Failed!"
      logger.info "partition: #{info[:partition]}, offset: #{info[:offset]}"
      logger.info "====Exception: #{ex}, backtrace: #{ex.backtrace}"
      report_exception_to_newrelic ex
    end

    ##
    # ==== Description
    # 新建一个消费者组
    #
    # NOTE：
    # 对于同一个消费者组内的不同消费者，
    # 同一时间内不能同时消费同一个partition.
    #
    # ==== Returns
    # Poseidon::ConsumerGroup#initialize(name, brokers, zookeepers, topic, options = {})
    #   ⇒ ConsumerGroup
    #
    # ==== Parameters
    # name:       消费者组的名字
    # brokers:    kafka集群地址
    # zookeepers: zookeepers集群地址
    # topic:      kafka消息的topic名称
    #
    # Options Hash (options):
    #     :max_bytes (Integer) —
    #     Maximum number of bytes to fetch. Default: 1048576 (1MB)
    #
    #     :max_wait_ms (Integer) —
    #     How long to block until the server sends us data. Default: 100 (100ms)
    #
    #     :min_bytes (Integer) —
    #     Smallest amount of data the server should send us. Default: 0 (Send us data as soon as it is ready)
    #
    #     :claim_timeout (Integer) —
    #     Maximum number of seconds to wait for a partition claim. Default: 10
    #
    #     :loop_delay (Integer) —
    #     Number of seconds to delay the next fetch (in #fetch_loop) if nothing was returned. Default: 1
    #
    #     :socket_timeout_ms (Integer) —
    #     broker connection wait timeout in ms. Default: 10000
    #
    #     :register (Boolean) —
    #     Automatically register instance and start consuming. Default: true
    #
    #     :trail (Boolean) —
    #     Starts reading messages from the latest partitions offsets and skips 'old' messages . Default: false
    ##
    def new_consumer_group(options = {})
      Poseidon::ConsumerGroup.new(group_name, brokers, zookeepers,
                                  topic, options)
    end

    ##
    # ==== Description
    # 异常信息抛到newrelic
    ##
    def report_exception_to_newrelic(exception)
      begin
        NewRelic::Agent.notice_error(exception)
      rescue Exception => e
        logger.info "抛出错误到newrelic信息时发生异常：#{e}, #{e.backtrace}"
      end
    end

  end
end
