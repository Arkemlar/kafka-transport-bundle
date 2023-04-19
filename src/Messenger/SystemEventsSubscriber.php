<?php

declare(strict_types=1);

namespace Arkemlar\KafkaTransport;

use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\Messenger\Event\SendMessageToTransportsEvent;
use Symfony\Component\Messenger\Event\WorkerMessageReceivedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;

class SystemEventsSubscriber implements EventSubscriberInterface
{
    public static function getSubscribedEvents()
    {
        return [
            WorkerStoppedEvent::class => 'onSystemGoDown',
            WorkerMessageReceivedEvent::class => 'onMessageReceived',

            SendMessageToTransportsEvent::class => 'onMessageSent',
            TerminateEvent::class => 'onSystemGoDown',
        ];
    }

//    public function __construct(
//        private SendersLocatorInterface $sendersLocator,
//    ) {
//    }

    public function __invoke(): void
    {
        // TODO: Implement __invoke() method.
    }

    public function onMessageReceived(): void
    {
    }

    public function onMessageSent(): void
    {
    }

    public function onSystemGoDown(): void
    {
    }
}
