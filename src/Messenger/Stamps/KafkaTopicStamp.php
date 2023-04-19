<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\Stamps;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class KafkaTopicStamp implements NonSendableStampInterface
{
    public function __construct(
        private readonly string $topic
    ) {
    }

    public function getTopicName(): string
    {
        return $this->topic;
    }
}
