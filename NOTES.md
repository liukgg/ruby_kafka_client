# TODOLISt

1,优化重启方式
原来的重启方式：  cat  /tmp/xxxx-kafka-#{consumer_name}.pid  | xargs kill -15
    其中， #{consumer_name}请替换为all_consumers中具体的消费者名称，比如“example”。

优化后重启方式： script/restart_kafka_consumer  example

```ruby

#!/usr/bin/env ruby
# encoding: utf-8
# 重启kafka消费者进程
#
# example:
#   script/restart_kafka_consumer  example_consumer

rails_root = File.expand_path("../../", __FILE__)
consumer_name =  ARGV[0]

if consumer_name.nil? || consumer_name.empty?
  puts "请传入参数：消费者名称"
else
  command = "cd #{rails_root};  cat /tmp/example_group-kafka-#{consumer_name}.pid  | xargs kill -15"
  puts "执行 #{command}"
  puts
  system command
end

```

2,god配置文件示例

```ruby
# god/example_config.god文件
# kafka消费者进程
# all_consumers = %w(
#   example
# )
#
# 请在该数组中添加需要启动和监控的kafka消费进程
all_consumers = %w(
  example
)

# RAILS_ENV=development god start -c god/example_config.god
all_consumers.each_with_index do |consumer_name, idx|
  God.watch do |w|
    pid_file  = "/tmp/example_group-kafka-#{consumer_name}.pid"
    start_cmd = "bundle exec rake kafka:consumer:run[#{consumer_name}] RAILS_ENV=#{RAILS_ENV} pidfile=#{pid_file} "
    stop_cmd  = "cat #{pid_file} | xargs kill -9"

    w.dir      = RAILS_ROOT
    w.group    = 'example_group-kafka'
    w.name     = "example_group-kafka-#{consumer_name}" # 必须唯一
    w.interval = 1.minute
    w.start    = start_cmd
    w.log      = "/tmp/god.rake.kafka.consumer.#{consumer_name}.log"
    w.pid_file = pid_file
    w.stop     = stop_cmd
    w.behavior(:clean_pid_file)

    w.transition(:init, { true => :up, false => :start }) do |on|
      on.condition(:process_running) do |c|
        c.running = true
      end
    end

    w.transition(:up, :restart) do |on|
      on.condition(:memory_usage) do |c|
        c.above = 1024.megabytes
        c.times = 2
      end
    end

    w.transition([:start, :restart], :up) do |on|
      on.condition(:process_running) do |c|
        c.running = true
        c.interval = 5.seconds
      end

      on.condition(:tries) do |c|
        c.times = 5
        c.transition = :start
        c.interval = 5.seconds
      end
    end

    w.transition(:up, :start) do |on|
      on.condition(:process_running) do |c|
        c.running = false
      end
    end

  end
end

```


3,god配置文件示例2
该配置文件支持：

优化后，如果kafka消费者有更新，需要进行如下操作：

1，bundle exec god restart  + 需要重启的进程在god监控中的名字

   （进程名字在god配置文件中以 w.name 声明，god启动后可以通过 god status 看到各个进程的状态）

例如：  bundle exec god restart   example_group-kafka-zhe_product_change_oos

2，重启god

bundle exec god terminate
bundle exec god -c god/example_config.god

之所以要先restart，然后还要重启god，有2个原因：

（1）bundle exec god terminate 无法确保god监控的所有进程都被杀死，所以要先用restart命令，确保需要重启的进程真正被重启。

（2）一个进程执行了一次 bundle exec god restart 之后，再次god  restart就不生效了，这个目前暂未找到具体原因，也可能是god本身的bug。通过重启god，确保下一次可以正常重启该进程。


```ruby
# kafka消费者进程
# all_consumers = %w(
#   example
# )
#
# 请在该数组中添加需要启动和监控的kafka消费进程
all_consumers = %w(
  example
)

# RAILS_ENV=development god start -c god/example.god
all_consumers.each_with_index do |consumer_name, idx|
  God.watch do |w|
    pid_file  = "/tmp/example_project-kafka-#{consumer_name}.pid"
    start_cmd = "bundle exec rake kafka:consumer:run[#{consumer_name}] RAILS_ENV=#{RAILS_ENV} pidfile=#{pid_file} "
    stop_cmd  = "sudo cat #{pid_file} | xargs kill -9"

    w.dir      = RAILS_ROOT
    w.group    = 'example_group-kafka'
    w.name     = "example_group-kafka-#{consumer_name}" # 必须唯一
    w.interval = 30.seconds
    w.start    = start_cmd
    w.log      = "/tmp/god.rake.kafka.consumer.#{consumer_name}.log"
    w.pid_file = pid_file
    w.stop     = stop_cmd
    w.behavior(:clean_pid_file)

    w.start_if do |start|
      start.condition(:process_running) do |c|
        c.interval = 5.seconds
        c.running = false
      end
    end

    w.restart_if do |restart|
      restart.condition(:memory_usage) do |c|
        c.above = 1024.megabytes
        c.times = [3, 5] # 3 out of 5 intervals
      end
    end

  end
end
```
