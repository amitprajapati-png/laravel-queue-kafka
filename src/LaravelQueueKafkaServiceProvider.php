<?php

namespace Rapide\LaravelQueueKafka;

use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\ServiceProvider;
use Rapide\LaravelQueueKafka\Queue\Connectors\KafkaConnector;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use App\Services\KafkaJobLogger;

class LaravelQueueKafkaServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/kafka.php',
            'queue.connections.kafka'
        );

        $this->registerDependencies();

        /*
         * Register MongoDB Kafka job logger.
         */
        $this->app->singleton(
            KafkaJobLogger::class,
            function () {
                return new KafkaJobLogger();
            }
        );
    }

    public function boot()
    {
        $queue = $this->app['queue'];

        $connector = new KafkaConnector($this->app);

        $queue->addConnector('kafka', function () use ($connector) {
            return $connector;
        });

        /*
         * JOB_FAILED must be captured here rather than directly
         * inside KafkaJob::fire().
         *
         * This is important because an exception can result in a
         * retry rather than a final failure.
         */
        $this->app['events']->listen(
            JobFailed::class,
            function (JobFailed $event) {
                if (!$event->job instanceof KafkaJob) {
                    return;
                }

                $job = $event->job;

                $this->app
                    ->make(KafkaJobLogger::class)
                    ->failed(
                        $job->getJobId(),
                        [
                            'job_name' => $job->getName(),
                            'queue' => $job->getQueue(),
                            'connection' => $event->connectionName,
                            'error' => $event->exception
                                ? $event->exception->getMessage()
                                : null,
                            'exception' => $event->exception
                                ? get_class($event->exception)
                                : null,
                            'partition' => $this->getPartition($job),
                            'offset' => $this->getOffset($job),
                        ]
                    );
            }
        );
    }

    protected function registerDependencies()
    {
        $this->app->bind(
            'queue.kafka.topic_conf',
            function () {
                return new \RdKafka\TopicConf();
            }
        );

        $this->app->bind(
            'queue.kafka.producer',
            function () {
                return new \RdKafka\Producer();
            }
        );

        $this->app->bind(
            'queue.kafka.conf',
            function () {
                return new \RdKafka\Conf();
            }
        );

        $this->app->bind(
            'queue.kafka.consumer',
            function ($app, $parameters) {
                return new \RdKafka\Consumer(
                    $parameters['conf']
                );
            }
        );
    }

    protected function getPartition(KafkaJob $job)
    {
        try {
            $reflection = new \ReflectionClass($job);

            $property = $reflection->getProperty('message');
            $property->setAccessible(true);

            $message = $property->getValue($job);

            return isset($message->partition)
                ? $message->partition
                : null;
        } catch (\Exception $exception) {
            return null;
        }
    }

    protected function getOffset(KafkaJob $job)
    {
        try {
            $reflection = new \ReflectionClass($job);

            $property = $reflection->getProperty('message');
            $property->setAccessible(true);

            $message = $property->getValue($job);

            return isset($message->offset)
                ? $message->offset
                : null;
        } catch (\Exception $exception) {
            return null;
        }
    }

    public function provides()
    {
        return [
            'queue.kafka.topic_conf',
            'queue.kafka.producer',
            'queue.kafka.consumer',
            'queue.kafka.conf',
            KafkaJobLogger::class,
        ];
    }
}
