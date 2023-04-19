<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use RdKafka\Conf as KafkaConf;

class KafkaSenderProperties
{
    public function __construct(
        public readonly KafkaConf $kafkaConf,
        public readonly string $topicName,

        public readonly int $flushTimeoutMs,
        public readonly int $flushRetries
    ) {
    }
}
