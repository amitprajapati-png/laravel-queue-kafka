<?php

namespace Rapide\LaravelQueueKafka\Queue\Jobs;

use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Database\DetectsDeadlocks;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\Jobs\JobName;
use Illuminate\Support\Str;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use App\Services\KafkaJobLogger;
use RdKafka\ConsumerTopic;
use RdKafka\Message;

class KafkaJob extends Job implements JobContract
{
    use DetectsDeadlocks;

    protected $connection;

    protected $queue;

    protected $message;

    protected $topic;

    /**
     * Number of times this same KafkaJob object has executed.
     */
    protected $executionAttempts = 0;

    /**
     * Last exception thrown by the job.
     */
    protected $lastException;

    /**
     * MongoDB logger.
     */
    protected $jobLogger;

    public function __construct(
        Container $container,
        KafkaQueue $connection,
        Message $message,
        $connectionName,
        $queue,
        ConsumerTopic $topic
    ) {
        $this->container = $container;

        $this->connection = $connection;

        $this->message = $message;

        $this->connectionName = $connectionName;

        $this->queue = $queue;

        $this->topic = $topic;

        $this->jobLogger = $container->make(
            KafkaJobLogger::class
        );
    }

    /**
     * Execute the Kafka job.
     */
    public function fire()
    {
        $payload = $this->payload();

        /*
         * Calculate current attempt.
         */
        $this->executionAttempts++;

        $attempt = max(
            $this->attempts(),
            $this->executionAttempts
        );

        /*
         * Store status = processing.
         */
        $this->jobLogger->processing(
            $this->getJobId(),
            [
                'attempt' => $attempt,
            ]
        );

        try {
            list($class, $method) = JobName::parse(
                $payload['job']
            );

            with(
                $this->instance = $this->resolve($class)
            )->{$method}(
                $this,
                $payload['data']
            );

            /*
             * If the job itself called release(), then it is not
             * considered completed.
             */
            if (
                method_exists($this, 'isReleased') &&
                $this->isReleased()
            ) {
                return;
            }

            /*
             * Job completed successfully.
             */
            $this->jobLogger->completed(
                $this->getJobId(),
                [
                    'attempt' => $attempt,
                ]
            );
        } catch (Exception $exception) {

            $this->lastException = $exception;

            /*
             * Deadlock handling.
             *
             * This retry happens inside the same Kafka message.
             */
            if (
                $this->causedByDeadlock($exception) ||
                Str::contains(
                    $exception->getMessage(),
                    ['detected deadlock']
                )
            ) {
                $this->jobLogger->retrying(
                    $this->getJobId(),
                    [
                        'attempt' => $attempt,
                    ]
                );

                sleep(
                    $this->connection
                        ->getConfig()['sleep_on_deadlock']
                );

                $this->fire();

                return;
            }

            /*
             * Laravel may retry the job.
             *
             * Final JOB_FAILED is recorded by the Laravel
             * JobFailed event listener.
             */
            throw $exception;
        }
    }

    /**
     * Return the current attempt number.
     */
    public function attempts()
    {
        $payload = $this->payload();

        return (
            isset($payload['attempts'])
                ? (int) $payload['attempts']
                : 0
        ) + 1;
    }

    /**
     * Get raw Kafka message body.
     */
    public function getRawBody()
    {
        return $this->message->payload;
    }

    /**
     * Acknowledge/delete Kafka message.
     */
    public function delete()
    {
        try {
            parent::delete();

            /*
             * Store Kafka offset only after Laravel considers the
             * message successfully deleted.
             */
            $this->topic->offsetStore(
                $this->message->partition,
                $this->message->offset
            );
        } catch (\RdKafka\Exception $exception) {
            throw new QueueKafkaException(
                'Could not delete job from the queue',
                0,
                $exception
            );
        }
    }

    /**
     * Release job for another attempt.
     */
    public function release($delay = 0)
    {
        /*
         * Delayed jobs are still unsupported by this driver.
         */
        if ($delay > 0) {
            throw new QueueKafkaException(
                'Later not yet implemented'
            );
        }

        parent::release($delay);

        $body = $this->payload();

        /*
         * Current attempt.
         */
        $attempt = max(
            $this->attempts(),
            $this->executionAttempts
        );

        /*
         * Store retry status.
         */
        $this->jobLogger->retrying(
            $this->getJobId(),
            [
                'attempt' => $attempt,
            ]
        );

        /*
         * Increment attempts in Kafka payload.
         *
         * First execution:
         *
         *     attempts = 0 in payload
         *     attempts() = 1
         *
         * First retry:
         *
         *     attempts = 1 in payload
         *     attempts() = 2
         */
        $body['attempts'] = $attempt;

        $payload = json_encode($body);

        /*
         * Produce the retry message FIRST.
         *
         * Same job ID is retained.
         */
        $this->connection->pushRaw(
            $payload,
            $this->getQueue(),
            [
                'log_created' => false,
            ]
        );

        /*
         * Now acknowledge the old Kafka message.
         */
        $this->delete();
    }

    /**
     * Return Kafka job ID.
     */
    public function getJobId()
    {
        if (
            isset($this->message->key) &&
            $this->message->key !== null &&
            $this->message->key !== ''
        ) {
            return $this->message->key;
        }

        /*
         * Fallback to payload ID.
         */
        $payload = $this->payload();

        return isset($payload['id'])
            ? $payload['id']
            : null;
    }

    /**
     * Get Kafka message.
     */
    public function getKafkaMessage()
    {
        return $this->message;
    }

    /**
     * Set Kafka job ID.
     */
    public function setJobId($id)
    {
        $this->connection->setCorrelationId($id);
    }

    /**
     * Get job class name from payload.
     */
    protected function getJobNameFromPayload(array $payload)
    {
        if (
            isset($payload['data']['commandName']) &&
            !empty($payload['data']['commandName'])
        ) {
            return $payload['data']['commandName'];
        }

        if (isset($payload['job'])) {
            try {
                list($class, $method) = JobName::parse(
                    $payload['job']
                );

                return $class;
            } catch (Exception $exception) {
                return $payload['job'];
            }
        }

        return null;
    }

    /**
     * Unserialize command.
     */
    private function unserialize(array $body)
    {
        try {
            return unserialize(
                $body['data']['command']
            );
        } catch (Exception $exception) {
            if (
                $this->causedByDeadlock($exception) ||
                Str::contains(
                    $exception->getMessage(),
                    ['detected deadlock']
                )
            ) {
                sleep(
                    $this->connection
                        ->getConfig()['sleep_on_deadlock']
                );

                return $this->unserialize($body);
            }

            throw $exception;
        }
    }
}
