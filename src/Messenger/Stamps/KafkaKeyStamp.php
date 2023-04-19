<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\Stamps;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class KafkaKeyStamp implements NonSendableStampInterface
{
    public function __construct(
        private readonly string $key
    ) {
    }

    public function getMessageKey(): string
    {
        return $this->key;
    }
}
