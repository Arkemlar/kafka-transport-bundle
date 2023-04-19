<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use RdKafka\Conf as KafkaConf;

interface KafkaConfigFactoryInterface
{
    /** @param array<string, string> $customConfigOptions */
    public function createRdKafkaProducerConf(array $customConfigOptions): KafkaConf;

    /** @param array<string, string> $customConfigOptions */
    public function createRdKafkaConsumerConf(array $customConfigOptions): KafkaConf;
}
