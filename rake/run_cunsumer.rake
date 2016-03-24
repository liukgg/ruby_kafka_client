# encoding: utf-8

##
# ==== NOTE
# 请不要随意改动该文件！！！
#
# ==== Descripton
# 所有kafka消费,应该实现run方法，在run里面运行自己的逻辑和处理。
# 请自行处理异常和记录日志，但是god会监控进程，在进程dead后重启kafka进程。
#
# 启动方式：
# bundle exec rake kafka:consumer:run[SellerOnlineOffline] RAILS_ENV=production
##
namespace :kafka do
  namespace :consumer do

  # rake kafka:consumer:run
  desc 'start kafka consumer'
  task :run, [:consumer] => [:environment] do |t, args|
    if args.consumer.blank?
      puts "请传入consumer名称参数！"
    else
      run_consumer(args.consumer)
    end
  end

  ##
  # ==== Descripton
  # kafka消费消息
  #
  # 商城topic： hm_product_update
  #
  # 参考：
  # https://github.com/bpot/poseidon
  # https://github.com/bsm/poseidon_cluster
  #
  # ==== Params
  # name: 下划线形式，如： 'seller_online_offline'
  ##
  def run_consumer(name)
    begin
      record_log "启动kafka-consumer, 参数： #{name.inspect}"
      record_log "rake-consumer-#{name}-pid: #{Process.pid}"
      consumer_class = "::Consumers::#{name.camelize}".constantize

      puts       "time: #{Time.now},启动kafka-consumer, consumer-class： #{consumer_class}"
      record_log "time: #{Time.now},启动kafka-consumer, consumer-class： #{consumer_class}"

      pid_file = ENV['pidfile'].to_s.strip
      if pid_file.present?
        File.open("#{pid_file}", 'w') { |f| f << Process.pid }
      end

      consumer_class.run
    rescue Exception => ex
      puts "#{Time.now}, consumer-#{name}, 启动失败或退出消费进程！异常信息：#{ex}"
      puts "#{ex.backtrace}"
      record_log "consumer-#{consumer_class}启动失败或退出消费进程！异常信息：#{ex}"
      record_log "#{ex.backtrace}"

      NewRelic::Agent.notice_error(ex)
    end
  end

  def record_log(info)
    logger = Logger.new "#{Rails.root}/log/kafka_consumer.#{Rails.env}.log"
    logger.formatter = proc do |severity, datetime, progname, msg|
      "[#{datetime.to_s(:db)}] #{info}\n"
    end
    logger.info "#{info}"
  end

  end
end
