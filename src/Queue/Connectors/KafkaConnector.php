<?php

namespace Rapide\LaravelQueueKafka\Queue\Connectors;

use Illuminate\Container\Container;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\TopicConf;

class KafkaConnector implements ConnectorInterface
{
    /**
     * @var Container
     */
    private $container;

    /**
     * KafkaConnector constructor.
     *
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        /** @var Conf $producerConf */
        $producerConf = $this->container->makeWith('queue.kafka.conf', []);

        $producerConf->set('bootstrap.servers', $config['brokers']);

        if (true === $config['sasl_enable']) {
            $producerConf->set('sasl.mechanisms', 'PLAIN');
            $producerConf->set('sasl.username', $config['sasl_plain_username']);
            $producerConf->set('sasl.password', $config['sasl_plain_password']);
            $producerConf->set('ssl.ca.location', $config['ssl_ca_location']);
        }

        $producer = new Producer($producerConf);


        /** @var Conf $consumerConf */
        $consumerConf = $this->container->makeWith('queue.kafka.conf', []);

        $consumerConf->set(
            'group.id',
            array_get($config, 'consumer_group_id', 'php-pubsub')
        );

        $consumerConf->set('bootstrap.servers', $config['brokers']);
        $consumerConf->set('enable.auto.commit', 'false');
        $consumerConf->set('auto.offset.reset', 'latest');

        if (true === $config['sasl_enable']) {
            $consumerConf->set('sasl.mechanisms', 'PLAIN');
            $consumerConf->set('sasl.username', $config['sasl_plain_username']);
            $consumerConf->set('sasl.password', $config['sasl_plain_password']);
            $consumerConf->set('ssl.ca.location', $config['ssl_ca_location']);
        }

        $consumer = $this->container->makeWith(
            'queue.kafka.consumer',
            ['conf' => $consumerConf]
        );

        return new KafkaQueue(
            $producer,
            $consumer,
            $config
        );
    }
}
