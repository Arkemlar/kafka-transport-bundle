<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use Arkemlar\KafkaTransport\Messenger\Stamps\KafkaGroupStamp;
use Arkemlar\KafkaTransport\Messenger\Stamps\KafkaTopicStamp;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class KafkaTransport implements TransportInterface, QueueReceiverInterface
{
    /** @var array<string, KafkaSender> */
    private array $sender;
    private string $defaultTopicName;

    /** @var array<string, KafkaReceiver> */
    private array $receiver;
    private string $defaultGroupId;

    public function __construct(
        private LoggerInterface $logger,
        private SerializerInterface $serializer,
        /** @var array<string,KafkaSenderProperties> */
        private array $kafkaSenderProperties,
        /** @var array<string,KafkaReceiverProperties> */
        private array $kafkaReceiverProperties
    ) {
        $this->defaultTopicName = array_key_first($kafkaSenderProperties);
        $this->defaultGroupId = array_key_first($kafkaReceiverProperties);
    }

    public function getFromQueues(array $queueNames): iterable
    {
        foreach ($queueNames as $queueName) {
            yield from $this->getReceiver($queueName)->get();
        }
    }

    public function get(): iterable
    {
        return $this->getReceiver($this->defaultGroupId)->get();
    }

    public function ack(Envelope $envelope): void
    {
        if ($stamp = $envelope->last(KafkaGroupStamp::class)) {
            $this->getReceiver($stamp->getGroupId())->ack($envelope);
            return;
        }
        $this->getReceiver($this->defaultGroupId)->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        if ($stamp = $envelope->last(KafkaGroupStamp::class)) {
            $this->getReceiver($stamp->getGroupId())->reject($envelope);
            return;
        }
        $this->getReceiver($this->defaultGroupId)->reject($envelope);
    }

    public function send(Envelope $envelope): Envelope
    {
        if ($stamp = $envelope->last(KafkaTopicStamp::class)) {
            return $this->getSender($stamp->getTopicName())->send($envelope);
        }

        return $this->getSender($this->defaultTopicName)->send($envelope);
    }

    private function getSender(string $topicName): KafkaSender
    {
        if (!array_key_exists($topicName, $this->kafkaSenderProperties)) {
            throw new InvalidArgumentException("Kafka topik with name \"{$topicName}\" is not registered");
        }

        return $this->sender[$topicName] ?? $this->sender[$topicName] = new KafkaSender(
            $this->logger,
            $this->serializer,
            $this->kafkaSenderProperties[$topicName]
        );
    }

    private function getReceiver(string $groupId): KafkaReceiver
    {
        if (!array_key_exists($groupId, $this->kafkaReceiverProperties)) {
            throw new InvalidArgumentException("Kafka group with id \"{$groupId}\" is not registered");
        }

        return $this->receiver[$groupId] ?? $this->receiver[$groupId] = new KafkaReceiver(
            $this->logger,
            $this->serializer,
            $this->kafkaReceiverProperties[$groupId]
        );
    }
}
