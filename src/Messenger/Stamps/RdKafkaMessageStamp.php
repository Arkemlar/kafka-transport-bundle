<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\Messenger\Stamps;

use RdKafka\Message as RdKafkaMessage;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Added by a kafka receiver to be able to ack (commit) the message.
 */
final class RdKafkaMessageStamp implements NonSendableStampInterface
{
    public function __construct(
        private readonly RdKafkaMessage $message
    ) {
    }

    public function getMessage(): RdKafkaMessage
    {
        return $this->message;
    }
}
