<?php

namespace Rapide\LaravelQueueKafka\Queue;

use ErrorException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Queue\Jobs\JobName;
use Log;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use App\Services\KafkaJobLogger;

class KafkaQueue extends Queue implements QueueContract
{
    protected $defaultQueue;

    protected $sleepOnError;

    protected $config;

    private $correlationId;

    private $producer;

    private $consumer;

    private $topics = [];

    private $queues = [];

    public function __construct(
        \RdKafka\Producer $producer,
        \RdKafka\Consumer $consumer,
        $config
    ) {
        $this->defaultQueue = $config['queue'];

        $this->sleepOnError = isset($config['sleep_on_error'])
            ? $config['sleep_on_error']
            : 5;

        $this->producer = $producer;

        $this->consumer = $consumer;

        $this->config = $config;
    }

    public function size($queue = null)
    {
        return 1;
    }

    /**
     * Push a new job.
     */
    public function push($job, $data = '', $queue = null)
    {
        /*
         * Every new logical job gets a new ID.
         */
        $this->correlationId = uniqid('', true);

        return $this->pushRaw(
            $this->createPayload($job, $queue, $data),
            $queue,
            [
                'log_created' => true,
            ]
        );
    }

    /**
     * Push raw payload to Kafka.
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        try {
            $topicName = $this->getQueueName($queue);

            /*
             * Decode payload so that we can get the logical job ID.
             */
            $payloadData = json_decode($payload, true);

            if (
                is_array($payloadData) &&
                isset($payloadData['id']) &&
                $payloadData['id']
            ) {
                $pushRawCorrelationId = $payloadData['id'];

                /*
                 * Keep the same logical job ID when a job is retried.
                 */
                $this->correlationId = $pushRawCorrelationId;
            } else {
                $pushRawCorrelationId = $this->getCorrelationId();

                if (
                    is_array($payloadData) &&
                    !isset($payloadData['id'])
                ) {
                    $payloadData['id'] = $pushRawCorrelationId;

                    $payload = json_encode($payloadData);
                }
            }

            $topic = $this->getTopic($queue);

            /*
             * Send the message to Kafka.
             */
            $topic->produce(
                RD_KAFKA_PARTITION_UA,
                0,
                $payload,
                $pushRawCorrelationId
            );

            /*
             * JOB_CREATED is only stored for the initial enqueue.
             *
             * Retries use:
             *     'log_created' => false
             */
            if (
                !isset($options['log_created']) ||
                $options['log_created'] === true
            ) {
                $this->storeJobCreated(
                    $pushRawCorrelationId,
                    $payloadData,
                    $queue
                );
            }

            return $pushRawCorrelationId;
        } catch (ErrorException $exception) {
            $this->reportConnectionError('pushRaw', $exception);
        }
    }

    /**
     * Store JOB_CREATED in MongoDB.
     */
    protected function storeJobCreated($jobId, $payload, $queue)
    {
        try {
            $jobName = isset($payload['job'])
                ? $payload['job']
                : null;

            $method = null;

            if ($jobName) {
                try {
                    list($jobClass, $jobMethod) = JobName::parse($jobName);

                    $jobName = $jobClass;
                    $method = $jobMethod;
                } catch (Exception $exception) {
                    // Keep original job name if it cannot be parsed.
                }
            }

            $this->getJobLogger()->created(
                $jobId,
                [
                    'job_name' => $jobName,
                    'method' => $method,
                    'queue' => $this->getQueueName($queue),
                    'connection' => isset($this->connectionName)
                        ? $this->connectionName
                        : null,
                ]
            );
        } catch (Exception $exception) {
            Log::error(
                'Unable to store Kafka JOB_CREATED event: ' .
                $exception->getMessage()
            );
        }
    }

    /**
     * Process delayed job.
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        throw new QueueKafkaException('Later not yet implemented');
    }

    /**
     * Get the next Kafka job.
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueueName($queue);

            if (!array_key_exists($queue, $this->queues)) {
                $this->queues[$queue] = $this->consumer->newQueue();

                $topicConf = new \RdKafka\TopicConf();

                $topicConf->set(
                    'auto.offset.reset',
                    'largest'
                );

                $this->topics[$queue] = $this->consumer->newTopic(
                    $queue,
                    $topicConf
                );

                $this->topics[$queue]->consumeQueueStart(
                    0,
                    RD_KAFKA_OFFSET_STORED,
                    $this->queues[$queue]
                );
            }

            $message = $this->queues[$queue]->consume(1000);

            if ($message === null) {
                return null;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:

                    return new KafkaJob(
                        $this->container,
                        $this,
                        $message,
                        $this->connectionName,
                        $queue,
                        $this->topics[$queue]
                    );

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    return null;

                default:
                    throw new QueueKafkaException(
                        $message->errstr(),
                        $message->err
                    );
            }
        } catch (\RdKafka\Exception $exception) {
            throw new QueueKafkaException(
                'Could not pop from the queue',
                0,
                $exception
            );
        }
    }

    private function getQueueName($queue)
    {
        return $queue ?: $this->defaultQueue;
    }

    private function getTopic($queue)
    {
        return $this->producer->newTopic(
            $this->getQueueName($queue)
        );
    }

    public function setCorrelationId($id)
    {
        $this->correlationId = $id;
    }

    public function getCorrelationId()
    {
        if (!$this->correlationId) {
            $this->correlationId = uniqid('', true);
        }

        return $this->correlationId;
    }

    public function getConfig()
    {
        return $this->config;
    }

    protected function createPayloadArray(
        $job,
        $queue = null,
        $data = ''
    ) {
        return array_merge(
            parent::createPayloadArray(
                $job,
                $queue,
                $data
            ),
            [
                'id' => $this->getCorrelationId(),
                'attempts' => 0,
            ]
        );
    }

    protected function reportConnectionError($action, Exception $e)
    {
        Log::error(
            'Kafka error while attempting ' .
            $action .
            ': ' .
            $e->getMessage()
        );

        if ($this->sleepOnError === false) {
            throw new QueueKafkaException(
                'Error writing data to the connection with Kafka'
            );
        }

        sleep($this->sleepOnError);
    }

    public function getConsumer()
    {
        return $this->consumer;
    }

    /**
     * Get MongoDB Kafka job logger.
     */
    protected function getJobLogger()
    {
        return $this->container->make(
            KafkaJobLogger::class
        );
    }
}
