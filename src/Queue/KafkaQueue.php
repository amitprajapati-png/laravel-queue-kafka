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
        \RdKafka\KafkaConsumer $consumer,
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
        $jobId = null;
    
        try {
            $topicName = $this->getQueueName($queue);
    
            $payloadData = json_decode($payload, true);
    
            /*
             * Existing ID = retry.
             * Missing ID = new job.
             */
            if (
                is_array($payloadData) &&
                isset($payloadData['id']) &&
                $payloadData['id']
            ) {
                $jobId = $payloadData['id'];
    
                /*
                 * Preserve the original ID for retry.
                 */
                $this->correlationId = $jobId;
            } else {
                $jobId = $this->getCorrelationId();
    
                if (
                    is_array($payloadData) &&
                    !isset($payloadData['id'])
                ) {
                    $payloadData['id'] = $jobId;
                    $payload = json_encode($payloadData);
                }
            }
    
            /*
             * Create lifecycle record only once.
             *
             * Retry must NOT create another Mongo document.
             */
            $logCreated = !isset($options['log_created'])
                || $options['log_created'] === true;
    
            if ($logCreated) {
                $this->storeJobCreated(
                    $jobId,
                    $payloadData,
                    $queue
                );
            }
    
            Log::info('KAFKA PRODUCE START', [
                'job_id' => $jobId,
                'topic' => $topicName,
            ]);
    
            $topic = $this->getTopic($queue);
    
            /*
             * Produce asynchronously.
             *
             * Do NOT flush here for every job.
             */
            $topic->produce(
                RD_KAFKA_PARTITION_UA,
                0,
                $payload,
                $jobId
            );
    
            /*
             * Let librdkafka process delivery events.
             */
            $this->producer->poll(0);
    
            Log::info('KAFKA PRODUCE QUEUED', [
                'job_id' => $jobId,
                'topic' => $topicName,
            ]);
    
            return $jobId;
    
        } catch (\Exception $exception) {
    
            Log::error('KAFKA PUSH ERROR', [
                'job_id' => $jobId,
                'error' => $exception->getMessage(),
            ]);
    
            /*
             * If the message could not even be queued into
             * librdkafka, mark the initial job as failed.
             */
            if (
                $jobId &&
                (!isset($options['log_created'])
                    || $options['log_created'] === true)
            ) {
                $this->jobLogger->failed(
                    $jobId,
                    [
                        'attempt' => 0,
                    ]
                );
            }
    
            throw new QueueKafkaException(
                'Could not push job to Kafka',
                0,
                $exception
            );
        }
    }

    /**
     * Store initial job information in MongoDB.
     *
     * One MongoDB document is maintained for the complete
     * lifecycle of the logical job.
     */
    protected function storeJobCreated($jobId, $payload, $queue)
    {
        try {
            $jobName = null;

            /*
             * Laravel stores the actual dispatched job class here.
             *
             * Example:
             * App\Jobs\LoginHistoryJob
             */
            if (
                isset($payload['data']['commandName']) &&
                !empty($payload['data']['commandName'])
            ) {
                $jobName = $payload['data']['commandName'];
            }

            /*
             * Fallback for non-command jobs.
             */
            if (!$jobName && isset($payload['job'])) {
                try {
                    list($jobName, $method) = JobName::parse(
                        $payload['job']
                    );
                } catch (Exception $exception) {
                    $jobName = $payload['job'];
                }
            }

            /*
             * The actual serialized Laravel job is stored inside:
             *
             *     data.command
             *
             * Example:
             *
             * O:24:"App\Jobs\LoginHistoryJob":...
             */
            $serializedPayload = isset($payload['data']['command'])
                ? $payload['data']['command']
                : null;

            $this->getJobLogger()->created(
                $jobId,
                [
                    'class_name' => $jobName,
                    'payload' => $serializedPayload,
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
    
            Log::info('KAFKA POP START', [
                'queue' => $queue,
            ]);
    
            if (!isset($this->queues[$queue])) {
    
                Log::info('KAFKA SUBSCRIBE START', [
                    'queue' => $queue,
                ]);
    
                $this->consumer->subscribe([$queue]);
    
                $this->queues[$queue] = true;
    
                Log::info('KAFKA SUBSCRIBE SUCCESS', [
                    'queue' => $queue,
                ]);
            }
    
            Log::info('KAFKA CONSUME START');
    
            $message = $this->consumer->consume(1000);
    
            Log::info('KAFKA CONSUME RETURNED', [
                'error' => $message ? $message->err : null,
                'error_string' => $message ? $message->errstr() : null,
                'partition' => $message ? $message->partition : null,
                'offset' => $message ? $message->offset : null,
            ]);
    
            if ($message === null) {
                return null;
            }
    
            switch ($message->err) {
    
                case RD_KAFKA_RESP_ERR_NO_ERROR:
    
                    Log::info('KAFKA MESSAGE RECEIVED', [
                        'partition' => $message->partition,
                        'offset' => $message->offset,
                    ]);
    
                    return new KafkaJob(
                        $this->container,
                        $this,
                        $message,
                        $this->connectionName,
                        $queue,
                        $this->consumer
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
    
            Log::error('KAFKA POP EXCEPTION', [
                'message' => $exception->getMessage(),
            ]);
    
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
