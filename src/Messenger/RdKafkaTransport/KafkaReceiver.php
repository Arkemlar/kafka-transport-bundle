<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\RdKafkaTransport;

use Arkemlar\KafkaTransport\Messenger\Stamps\KafkaGroupStamp;
use Arkemlar\KafkaTransport\Messenger\Stamps\KafkaKeyStamp;
use Arkemlar\KafkaTransport\Messenger\Stamps\KafkaTopicStamp;
use Arkemlar\KafkaTransport\Messenger\Stamps\RdKafkaMessageStamp;
use Exception;
use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class KafkaReceiver implements ReceiverInterface
{
    private LoggerInterface $logger;
    private SerializerInterface $serializer;
    private KafkaReceiverProperties $properties;
    private RdKafkaConsumer $consumer;
    private bool $subscribed;

    public function __construct(
        LoggerInterface $logger,
        SerializerInterface $serializer,
        KafkaReceiverProperties $properties,
    ) {
        $this->logger = $logger;
        $this->serializer = $serializer;
        $this->properties = $properties;
        $this->properties->kafkaConf->set('group.id', $properties->groupId);

        $this->subscribed = false;
    }

    public function get(): iterable
    {
        try {
            $message = $this->getSubscribedConsumer()->consume($this->properties->receiveTimeoutMs);
        } catch (Exception $e) {
            throw new TransportException('Kafka exception: ' . $e->getMessage(), $e->getCode(), $e);
        }

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $envelope = $this->serializer->decode([
                    'body' => $message->payload,
                    'headers' => $message->headers ?? [],
                    'key' => $message->key,
                    'offset' => $message->offset,
                    'timestamp' => $message->timestamp,
                ]);

                $this->logger->info(sprintf(
                    'Kafka: Message %s%s received from topic=%s partition=%s offset=%s',
                    $envelope->getMessage()::class,
                    null === $message->key ? '' : ' with key ' . $message->key,
                    $message->topic_name,
                    $message->partition,
                    $message->offset
                ));

                $envelope = $envelope
                    ->with(new RdKafkaMessageStamp($message))
                    ->with(new KafkaTopicStamp($message->topic_name))
                    ->with(new KafkaGroupStamp($this->properties->groupId));

                if (null !== $message->key) {
                    $envelope = $envelope->with(new KafkaKeyStamp($message->key));
                }

                return [$envelope];

            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $this->logger->info('Kafka: Partition EOF reached. Waiting for next message...');

                break;

            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->logger->info('Kafka: Consumer timeout');

                break;

            case RD_KAFKA_RESP_ERR__TRANSPORT:
                $this->logger->error('Kafka: Broker transport failure');

                break;

            case RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET:
                $this->logger->error('Kafka: Desired offset is out of range and "auto.offset.reset" setting is set to "error"');

                break;

            default:
                throw new TransportException($message->errstr(), $message->err);
        }

        return [];
    }

    public function ack(Envelope $envelope): void
    {
        $consumer = $this->getConsumer();

        /** @var RdKafkaMessageStamp $transportStamp */
        $transportStamp = $envelope->last(RdKafkaMessageStamp::class);
        $message = $transportStamp->getMessage();

        if ($this->properties->commitAsync) {
            $consumer->commitAsync($message);

            $this->logger->info(sprintf(
                'Kafka: Offset topic=%s partition=%s offset=%s to be committed asynchronously',
                $message->topic_name,
                $message->partition,
                $message->offset
            ));
        } else {
            $consumer->commit($message);

            $this->logger->info(sprintf(
                'Kafka: Offset topic=%s partition=%s offset=%s successfully committed',
                $message->topic_name,
                $message->partition,
                $message->offset
            ));
        }
    }

    public function reject(Envelope $envelope): void
    {
        // Do nothing. Auto commit should be set to false!
    }

    private function getSubscribedConsumer(): RdKafkaConsumer
    {
        $consumer = $this->getConsumer();

        if (false === $this->subscribed) {
            $this->logger->info(sprintf(
                'Kafka: Subscribing to topics "%s" with group id "%s"',
                implode(',', $this->properties->topicNames),
                $this->properties->groupId,
            ));
            $consumer->subscribe($this->properties->topicNames);

            $this->subscribed = true;
        }

        return $consumer;
    }

    private function getConsumer(): RdKafkaConsumer
    {
        return $this->consumer ??= new RdKafkaConsumer($this->properties->kafkaConf);
    }
}
