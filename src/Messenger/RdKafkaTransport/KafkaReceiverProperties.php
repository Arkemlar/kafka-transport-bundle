<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use RdKafka\Conf as KafkaConf;

class KafkaReceiverProperties
{
    public function __construct(
        public readonly KafkaConf $kafkaConf,
        /** @var string[] */
        public readonly array $topicNames,
        public readonly string $groupId,

        public readonly int $receiveTimeoutMs,
        public readonly bool $commitAsync
    ) {
    }
}
