<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use Arkemlar\KafkaTransport\Messenger\Stamps\KafkaKeyStamp;
use Exception;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class KafkaSender implements SenderInterface
{
    private LoggerInterface $logger;
    private SerializerInterface $serializer;
    private KafkaSenderProperties $properties;
    private RdKafkaProducer $producer;
    private RdKafkaProducerTopic $topic;

    public function __construct(
        LoggerInterface $logger,
        SerializerInterface $serializer,
        KafkaSenderProperties $properties
    ) {
        $this->logger = $logger;
        $this->serializer = $serializer;
        $this->properties = $properties;
    }

    public function send(Envelope $envelope): Envelope
    {
        $producer = $this->getProducer();
        $this->topic ??= $producer->newTopic($this->properties->topicName);

        $key = $envelope->last(KafkaKeyStamp::class)?->getMessageKey();
        $payload = $this->serializer->encode($envelope);

        try {
            if (method_exists($this->topic, 'producev')) {
                $this->topic->producev(
                    RD_KAFKA_PARTITION_UA,
                    0,
                    $payload['body'],
                    $key,
                    $payload['headers'] ?? null,
                    $payload['timestamp_ms'] ?? null
                );
            } else {
                $this->topic->produce(
                    RD_KAFKA_PARTITION_UA,
                    0,
                    $payload['body'],
                    $key
                );
            }
        } catch (Exception $e) {
            throw new TransportException('Kafka exception: ' . $e->getMessage(), $e->getCode(), $e);
        }

        $producer->poll(0);

        $this->flushMessages($producer, $this->properties->flushRetries);

        $this->logger->info(
            sprintf(
                'Kafka: Message %s sent%s',
                $envelope->getMessage()::class,
                null === $key ? '' : ' with key ' . $key,
            )
        );

        return $envelope;
    }

    private function flushMessages(RdKafkaProducer $producer, int $retriesLeft): void
    {
        $err = $producer->flush($this->properties->flushTimeoutMs);
        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $err) {
            if ($retriesLeft > 0) {
                $this->logger->info('Kafka: Message not sent, retries left ' . $retriesLeft);
                $this->flushMessages($producer, --$retriesLeft);
            } else {
                throw new TransportException('Kafka: Message not sent, no retries left! Last error returned by producer: ' . rd_kafka_err2str($err), $err);
            }
        }
    }

    private function getProducer(): RdKafkaProducer
    {
        return $this->producer ??= new RdKafkaProducer($this->properties->kafkaConf);
    }
}
