<?php declare(strict_types=1);

namespace Arkemlar\KafkaTransport\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

final class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder('kafka_transport');

//        $treeBuilder->getRootNode()
//            ->children()
//                ->scalarNode('transport_configurator')->defaultValue('kafka_transport.transport_factory.configurator')->end()
//                ->arrayNode('producers')
//                    ->children()
//                        ->integerNode('client_id')->end()
//                        ->scalarNode('client_secret')->end()
//                    ->end()
//                ->end() // twitter
//            ->end()
//        ;

        return $treeBuilder;
    }
}
