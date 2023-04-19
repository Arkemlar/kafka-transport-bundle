<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\Defaults;

use Arkemlar\KafkaTransport\Messenger\RdKafkaTransport\KafkaConfigFactoryInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RdKafka;
use RdKafka\Conf as KafkaConf;
use RuntimeException;

// Feel free to override this class to your needs in your project or use it as starting point to make your own configurator!
class KafkaConfigFactory implements KafkaConfigFactoryInterface
{
    protected LoggerInterface $logger;

    /** @var array{error?: bool, rebalance?: bool, stats?: bool, offset_commit?: bool, delivery_report?: bool, log?: bool} */
    protected array $enabledCallbacks;

    public function __construct(
        ?LoggerInterface $logger,
        string $enableCallbacks = 'error,rebalance,stats,offset_commit,delivery_report,log',
    ) {
        $this->logger = $logger ?? new NullLogger();
        $this->enabledCallbacks = array_fill_keys(explode(',', $enableCallbacks), true);
    }

    /** @param array<string, string> $customConfigOptions */
    public function createRdKafkaProducerConf(array $customConfigOptions): KafkaConf
    {
        $conf = new KafkaConf();

        if ($this->enabledCallbacks['error'] ?? false) {
            $this->addErrorCb($conf);
        }
        if ($this->enabledCallbacks['delivery_report'] ?? false) {  // producer only
            $this->addDeliveryReportCb($conf);
        }
        if ($this->enabledCallbacks['stats'] ?? false) {
            $this->addStatsCb($conf);
        }
        if ($this->enabledCallbacks['log'] ?? false) {
            $this->addLogCb($conf);
        }

        $this->clientCodeCustomisations($conf, $customConfigOptions);

        return $conf;
    }

    /** @param array<string, string> $customConfigOptions */
    public function createRdKafkaConsumerConf(array $customConfigOptions): KafkaConf
    {
        $conf = new KafkaConf();

        if ($this->enabledCallbacks['error'] ?? false) {
            $this->addErrorCb($conf);
        }
        if ($this->enabledCallbacks['rebalance'] ?? false) {    // consumer only
            $this->addRebalanceCb($conf);
        }
        if ($this->enabledCallbacks['offset_commit'] ?? false) {   // consumer only
            $this->addOffsetCommitCb($conf);
        }
        if ($this->enabledCallbacks['stats'] ?? false) {
            $this->addStatsCb($conf);
        }
        if ($this->enabledCallbacks['log'] ?? false) {
            $this->addLogCb($conf);
        }

        $this->clientCodeCustomisations($conf, $customConfigOptions);

        return $conf;
    }

    /**
     * @param array<string, string> $customConfigOptions
     */
    protected function clientCodeCustomisations(KafkaConf $conf, array $customConfigOptions): void
    {
        foreach ($customConfigOptions ?? [] as $key => $value) {
            $conf->set($key, (string) $value);
        }
    }

    protected function addErrorCb(KafkaConf $conf): void
    {
        // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-conf.seterrorcb.html
        $conf->setErrorCb(fn (RdKafka\KafkaConsumer|RdKafka\Producer $kafka, int $err, string $reason) => $this->logger->error(
            sprintf('Kafka cb: error "%s" (reason "%s")', rd_kafka_err2str($err), $reason)
        ));
    }

    /** Consumer only */
    protected function addRebalanceCb(KafkaConf $conf): void
    {
        // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-conf.setrebalancecb.html
        $conf->setRebalanceCb(
            function (RdKafka\KafkaConsumer $kafka, int $err, array $topicPartitions = null): void {
                /** @var RdKafka\TopicPartition[] $topicPartitions */
                $topicPartitions ??= [];

                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        foreach ($topicPartitions as $topicPartition) {
                            $this->logger->info(
                                sprintf(
                                    'Kafka cb: assign t=%s p=%s o=%s',
                                    $topicPartition->getTopic(),
                                    $topicPartition->getPartition(),
                                    $topicPartition->getOffset()
                                )
                            );
                        }
                        $kafka->assign($topicPartitions);

                        break;

                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        foreach ($topicPartitions as $topicPartition) {
                            $this->logger->info(
                                sprintf(
                                    'Kafka cb: assign t=%s p=%s o=%s',
                                    $topicPartition->getTopic(),
                                    $topicPartition->getPartition(),
                                    $topicPartition->getOffset()
                                )
                            );
                        }
                        $kafka->assign(null);

                        break;

                    default:
                        $kafka->assign(null);

                        throw new RuntimeException(rd_kafka_err2str($err));
                }
            }
        );
    }

    /** Consumer only */
    protected function addOffsetCommitCb(KafkaConf $conf): void
    {
        // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-conf.setoffsetcommitcb.html
        $conf->setOffsetCommitCb(
            function (RdKafka\KafkaConsumer $kafka, int $err, array $partitions): void {
                if ($err) {
                    // offset commit failed (you may try to store it another way and use this information to prevent repeated consumption)
                    $this->logger->error('Kafka cb: offset commit failed: ' . rd_kafka_err2str($err));
                } else {
                    $this->logger->debug('Kafka cb: offset commit succeeded');
                }
            }
        );
    }

    /** Producer only */
    protected function addDeliveryReportCb(KafkaConf $conf): void
    {
        // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-conf.setdrmsgcb.html
        $conf->setDrMsgCb(
            function (RdKafka $kafka, RdKafka\Message $message): void {
                if ($message->err) {
                    // permanent error occured or retry count for temporary errors exhausted
                    $this->logger->error('Kafka cb: message delivery failed: ' . $message->errstr());
                } else {
                    // message successfully delivered
                    $this->logger->debug('Kafka cb: message delivery succeeded');
                }
            }
        );
    }

    protected function addStatsCb(KafkaConf $conf): void
    {
        // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-conf.setstatscb.html
        $conf->setStatsCb(function (RdKafka\KafkaConsumer|RdKafka\Producer $kafka, string $json, int $json_len): void {
            $this->logger->debug('Kafka cb: stats', ['json' => $json]);
        });
        $conf->set('statistics.interval.ms', '5000');
    }

    protected function addLogCb(KafkaConf $conf): void
    {
        // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-conf.setlogcb.html
        $conf->set('log.queue', 'true');
        $conf->setLogCb(
            fn (RdKafka\KafkaConsumer|RdKafka\Producer $kafka, int $level, string $facility, string $message) => $this->logger->log(
                $level,
                'Kafka log: ' . $message
            )
        );
    }
}
