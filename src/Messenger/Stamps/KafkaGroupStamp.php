<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\Stamps;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class KafkaGroupStamp implements NonSendableStampInterface
{
    public function __construct(
        private readonly string $groupId
    ) {
    }

    public function getGroupId(): string
    {
        return $this->groupId;
    }
}
