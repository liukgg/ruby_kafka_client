# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ruby_kafka_client/version'

Gem::Specification.new do |spec|
  spec.name          = "ruby_kafka_client"
  spec.version       = RubyKafkaClient::VERSION
  spec.authors       = ["liukun"]
  spec.email         = ["liuk1991@sina.com"]
  spec.summary       = %q{A ruby kafka client for producing and consuming kafka messages based on poseidon_cluster }
  spec.description   = %q{A ruby kafka client for producing and consuming kafka messages based on poseidon_cluster.Focused on how to use kafka in production evironment. }
  spec.homepage      = "https://github.com/liukgg/ruby_kafka_client"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'poseidon_cluster'

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"

end
