<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use function explode;
use function str_replace;
use function str_starts_with;

class KafkaTransportFactory implements TransportFactoryInterface
{
    private const DSN_PROTOCOLS = [
        self::DSN_PROTOCOL_KAFKA,
        self::DSN_PROTOCOL_KAFKA_SSL,
    ];
    private const DSN_PROTOCOL_KAFKA = 'kafka://';
    private const DSN_PROTOCOL_KAFKA_SSL = 'kafka+ssl://';

    private LoggerInterface $logger;
    private KafkaConfigFactoryInterface $configFactory;

    public function __construct(
        ?LoggerInterface $logger,
        KafkaConfigFactoryInterface $configFactory,
    ) {
        $this->logger = $logger ?? new NullLogger();
        $this->configFactory = $configFactory;
    }

    public function supports(string $dsn, array $options): bool
    {
        foreach (self::DSN_PROTOCOLS as $protocol) {
            if (str_starts_with($dsn, $protocol)) {
                return true;
            }
        }

        return false;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $brokers = $this->stripProtocol($dsn);
        $additionalKafkaConfigOptions = [
            'metadata.broker.list' => implode(',', $brokers),
        ] + $options['kafka_conf'];

        $sendersProperties = [];
        foreach ($options['topics'] as $topicName => $topicSettings) {
            $sendersProperties[$topicName] = new KafkaSenderProperties(
                $this->configFactory->createRdKafkaProducerConf( ($topicSettings['kafka_conf'] ?? []) + ($options['producer']['kafka_conf'] ?? []) + $additionalKafkaConfigOptions),
                $topicName,
                $options['producer']['flushTimeout'] ?? 10000,
                $options['producer']['flushRetries'] ?? 3
            );
        }

        $receiverProperties = [];
        foreach ($options['groups'] as $groupId => $groupSettings) {
            $receiverProperties[$groupId] = new KafkaReceiverProperties(
                $this->configFactory->createRdKafkaConsumerConf(($groupSettings['kafka_conf'] ?? []) + ($options['consumer']['kafka_conf'] ?? []) + $additionalKafkaConfigOptions),
                $groupSettings['topics'],
                $groupId,
                $options['consumer']['receiveTimeout'] ?? 10000,
                $options['consumer']['commitAsync'] ?? false
            );
        }

        return new KafkaTransport(
            $this->logger,
            $serializer,
            $sendersProperties,
            $receiverProperties
        );
    }

    private function stripProtocol(string $dsn): array
    {
        $brokers = [];
        foreach (explode(',', $dsn) as $currentBroker) {
            foreach (self::DSN_PROTOCOLS as $protocol) {
                $currentBroker = str_replace($protocol, '', $currentBroker);
            }
            $brokers[] = $currentBroker;
        }

        return $brokers;
    }
}
