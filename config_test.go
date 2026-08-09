package activitylogmq

import "testing"

func clearBrokerEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"MESSAGE_BROKER",
		"ACTIVITY_LOG_QUEUE",
		"ACTIVITY_LOG_TOPIC",
		"PUBSUB_TOPIC",
		"KAFKA_TOPIC",
		"RABBITMQ_URL",
		"RABBITMQ_HOST",
		"RABBITMQ_USER",
		"RABBITMQ_PASSWORD",
		"RABBITMQ_PORT",
		"RABBITMQ_VHOST",
		"AMAZONMQ_URL",
		"AMAZON_MQ_URL",
		"AMAZONMQ_HOST",
		"AMAZON_MQ_HOST",
		"AMAZONMQ_USER",
		"AMAZON_MQ_USER",
		"AMAZONMQ_PASSWORD",
		"AMAZON_MQ_PASSWORD",
		"AMAZONMQ_PORT",
		"AMAZON_MQ_PORT",
		"AMAZONMQ_VHOST",
		"AMAZON_MQ_VHOST",
		"AMAZONMQ_TLS",
		"AMAZON_MQ_TLS",
		"PUBSUB_PROJECT_ID",
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadConfig_DefaultTopic(t *testing.T) {
	clearBrokerEnv(t)

	cfg := LoadConfig()
	if cfg.Topic != defaultTopic {
		t.Fatalf("topic = %q, want %q", cfg.Topic, defaultTopic)
	}
	if cfg.Enabled() {
		t.Fatal("expected disabled config with no broker env")
	}
}

func TestLoadConfig_TopicFromEnv(t *testing.T) {
	tests := []struct {
		name string
		set  func(t *testing.T)
		want string
	}{
		{
			name: "activity log queue",
			set: func(t *testing.T) {
				t.Setenv("ACTIVITY_LOG_QUEUE", "custom.queue")
			},
			want: "custom.queue",
		},
		{
			name: "activity log topic alias",
			set: func(t *testing.T) {
				t.Setenv("ACTIVITY_LOG_TOPIC", "custom.topic")
			},
			want: "custom.topic",
		},
		{
			name: "pubsub topic alias",
			set: func(t *testing.T) {
				t.Setenv("PUBSUB_TOPIC", "pubsub.topic")
			},
			want: "pubsub.topic",
		},
		{
			name: "kafka topic alias",
			set: func(t *testing.T) {
				t.Setenv("KAFKA_TOPIC", "kafka.topic")
			},
			want: "kafka.topic",
		},
		{
			name: "queue wins over topic",
			set: func(t *testing.T) {
				t.Setenv("ACTIVITY_LOG_QUEUE", "queue-wins")
				t.Setenv("ACTIVITY_LOG_TOPIC", "topic-loses")
			},
			want: "queue-wins",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearBrokerEnv(t)
			tt.set(t)

			cfg := LoadConfig()
			if cfg.Topic != tt.want {
				t.Fatalf("topic = %q, want %q", cfg.Topic, tt.want)
			}
		})
	}
}

func TestLoadConfig_BrokerSelection(t *testing.T) {
	tests := []struct {
		name    string
		set     func(t *testing.T)
		want    Broker
		enabled bool
	}{
		{
			name: "explicit rabbitmq",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "rabbitmq")
				t.Setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
			},
			want:    BrokerRabbitMQ,
			enabled: true,
		},
		{
			name: "auto-detect rabbitmq from host",
			set: func(t *testing.T) {
				t.Setenv("RABBITMQ_HOST", "rabbitmq")
			},
			want:    BrokerRabbitMQ,
			enabled: true,
		},
		{
			name: "explicit pubsub",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "google_pubsub")
				t.Setenv("PUBSUB_PROJECT_ID", "my-gcp-project")
			},
			want:    BrokerPubSub,
			enabled: true,
		},
		{
			name: "auto-detect pubsub",
			set: func(t *testing.T) {
				t.Setenv("PUBSUB_PROJECT_ID", "my-gcp-project")
			},
			want:    BrokerPubSub,
			enabled: true,
		},
		{
			name: "explicit kafka",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "kafka")
				t.Setenv("KAFKA_BROKERS", "kafka:9092")
			},
			want:    BrokerKafka,
			enabled: true,
		},
		{
			name: "explicit amazonmq",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "amazonmq")
				t.Setenv("AMAZONMQ_URL", "amqps://user:pass@mq.example:5671/")
			},
			want:    BrokerAmazonMQ,
			enabled: true,
		},
		{
			name: "auto-detect amazonmq from host",
			set: func(t *testing.T) {
				t.Setenv("AMAZONMQ_HOST", "b-xxx.mq.us-east-1.amazonaws.com")
				t.Setenv("AMAZONMQ_USER", "mquser")
				t.Setenv("AMAZONMQ_PASSWORD", "secret")
			},
			want:    BrokerAmazonMQ,
			enabled: true,
		},
		{
			name: "kafka consumer group default",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "kafka")
				t.Setenv("KAFKA_BROKERS", "kafka:9092")
			},
			want:    BrokerKafka,
			enabled: true,
		},
		{
			name: "message broker wins over auto-detect",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "kafka")
				t.Setenv("KAFKA_BROKERS", "kafka:9092")
				t.Setenv("PUBSUB_PROJECT_ID", "ignored-project")
			},
			want:    BrokerKafka,
			enabled: true,
		},
		{
			name: "rabbitmq without credentials",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "rabbitmq")
			},
			want:    BrokerRabbitMQ,
			enabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearBrokerEnv(t)
			tt.set(t)

			cfg := LoadConfig()
			if cfg.Broker != tt.want {
				t.Fatalf("broker = %q, want %q", cfg.Broker, tt.want)
			}
			if cfg.Enabled() != tt.enabled {
				t.Fatalf("enabled = %v, want %v", cfg.Enabled(), tt.enabled)
			}
		})
	}
}

func TestLoadConfig_RabbitMQURI(t *testing.T) {
	tests := []struct {
		name string
		set  func(t *testing.T)
		want string
	}{
		{
			name: "full url",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "rabbitmq")
				t.Setenv("RABBITMQ_URL", "amqp://root:secret@rabbitmq:5672/ilonapay")
			},
			want: "amqp://root:secret@rabbitmq:5672/ilonapay",
		},
		{
			name: "host with default vhost",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "rabbitmq")
				t.Setenv("RABBITMQ_HOST", "rabbitmq")
				t.Setenv("RABBITMQ_USER", "root")
				t.Setenv("RABBITMQ_PASSWORD", "s@cret")
				t.Setenv("RABBITMQ_PORT", "5672")
			},
			want: "amqp://root:s%40cret@rabbitmq:5672/",
		},
		{
			name: "host with custom vhost",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "rabbitmq")
				t.Setenv("RABBITMQ_HOST", "rabbitmq")
				t.Setenv("RABBITMQ_VHOST", "/ilonapay")
			},
			want: "amqp://guest:guest@rabbitmq:5672/ilonapay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearBrokerEnv(t)
			tt.set(t)

			cfg := LoadConfig()
			if cfg.RabbitMQURI != tt.want {
				t.Fatalf("RabbitMQURI = %q, want %q", cfg.RabbitMQURI, tt.want)
			}
		})
	}
}

func TestLoadConfig_KafkaBrokersAndConsumerGroup(t *testing.T) {
	clearBrokerEnv(t)
	t.Setenv("MESSAGE_BROKER", "kafka")
	t.Setenv("KAFKA_BROKERS", " broker-a:9092 , broker-b:9092 , ")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")

	cfg := LoadConfig()
	if cfg.Broker != BrokerKafka {
		t.Fatalf("broker = %q, want %q", cfg.Broker, BrokerKafka)
	}
	if len(cfg.KafkaBrokers) != 2 {
		t.Fatalf("KafkaBrokers = %#v, want 2 brokers", cfg.KafkaBrokers)
	}
	if cfg.KafkaBrokers[0] != "broker-a:9092" || cfg.KafkaBrokers[1] != "broker-b:9092" {
		t.Fatalf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
	if cfg.KafkaConsumerGroup != "custom-group" {
		t.Fatalf("KafkaConsumerGroup = %q, want custom-group", cfg.KafkaConsumerGroup)
	}
}

func TestLoadConfig_AmazonMQURI(t *testing.T) {
	tests := []struct {
		name string
		set  func(t *testing.T)
		want string
	}{
		{
			name: "full url",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "amazonmq")
				t.Setenv("AMAZONMQ_URL", "amqps://user:pass@b-xxx.mq.us-east-1.amazonaws.com:5671/")
			},
			want: "amqps://user:pass@b-xxx.mq.us-east-1.amazonaws.com:5671/",
		},
		{
			name: "amazon_mq_url alias",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "amazon_mq")
				t.Setenv("AMAZON_MQ_URL", "amqps://user:pass@broker:5671/vhost")
			},
			want: "amqps://user:pass@broker:5671/vhost",
		},
		{
			name: "host defaults to amqps 5671",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "amazonmq")
				t.Setenv("AMAZONMQ_HOST", "b-xxx.mq.us-east-1.amazonaws.com")
				t.Setenv("AMAZONMQ_USER", "mquser")
				t.Setenv("AMAZONMQ_PASSWORD", "p@ss")
			},
			want: "amqps://mquser:p%40ss@b-xxx.mq.us-east-1.amazonaws.com:5671/",
		},
		{
			name: "tls disabled uses amqp 5672",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "amazonmq")
				t.Setenv("AMAZONMQ_HOST", "localhost")
				t.Setenv("AMAZONMQ_TLS", "false")
			},
			want: "amqp://guest:guest@localhost:5672/",
		},
		{
			name: "custom vhost",
			set: func(t *testing.T) {
				t.Setenv("MESSAGE_BROKER", "amq")
				t.Setenv("AMAZONMQ_HOST", "broker.example")
				t.Setenv("AMAZONMQ_VHOST", "/ilonapay")
			},
			want: "amqps://guest:guest@broker.example:5671/ilonapay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearBrokerEnv(t)
			tt.set(t)

			cfg := LoadConfig()
			if cfg.Broker != BrokerAmazonMQ {
				t.Fatalf("broker = %q, want %q", cfg.Broker, BrokerAmazonMQ)
			}
			if cfg.RabbitMQURI != tt.want {
				t.Fatalf("RabbitMQURI = %q, want %q", cfg.RabbitMQURI, tt.want)
			}
			if !cfg.Enabled() {
				t.Fatal("expected enabled amazonmq config")
			}
		})
	}
}

func TestNormalizeBroker(t *testing.T) {
	tests := []struct {
		in   string
		want Broker
	}{
		{"rabbitmq", BrokerRabbitMQ},
		{"AMQP", BrokerRabbitMQ},
		{"amazonmq", BrokerAmazonMQ},
		{"amazon_mq", BrokerAmazonMQ},
		{"amazon-mq", BrokerAmazonMQ},
		{"amq", BrokerAmazonMQ},
		{"pubsub", BrokerPubSub},
		{"google", BrokerPubSub},
		{"kafka", BrokerKafka},
		{"unknown", ""},
	}

	for _, tt := range tests {
		if got := normalizeBroker(tt.in); got != tt.want {
			t.Fatalf("normalizeBroker(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestConfig_Enabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{
			name: "rabbitmq configured",
			cfg:  Config{Broker: BrokerRabbitMQ, RabbitMQURI: "amqp://localhost/"},
			want: true,
		},
		{
			name: "rabbitmq missing uri",
			cfg:  Config{Broker: BrokerRabbitMQ},
			want: false,
		},
		{
			name: "amazonmq configured",
			cfg:  Config{Broker: BrokerAmazonMQ, RabbitMQURI: "amqps://broker:5671/"},
			want: true,
		},
		{
			name: "amazonmq missing uri",
			cfg:  Config{Broker: BrokerAmazonMQ},
			want: false,
		},
		{
			name: "pubsub configured",
			cfg:  Config{Broker: BrokerPubSub, PubSubProjectID: "project"},
			want: true,
		},
		{
			name: "kafka configured",
			cfg:  Config{Broker: BrokerKafka, KafkaBrokers: []string{"localhost:9092"}},
			want: true,
		},
		{
			name: "unsupported broker",
			cfg:  Config{Broker: Broker("redis")},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.Enabled(); got != tt.want {
				t.Fatalf("Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}
