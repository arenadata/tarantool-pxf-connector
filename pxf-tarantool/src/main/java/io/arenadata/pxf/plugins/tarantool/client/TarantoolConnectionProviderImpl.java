package io.arenadata.pxf.plugins.tarantool.client;

import io.tarantool.driver.*;

public class TarantoolConnectionProviderImpl implements TarantoolConnectionProvider {
    @Override
    public TarantoolConnection provide(TarantoolClientConfig config,
                                       TarantoolClusterAddressProvider discoveryClientProvider) {
        ProxyTarantoolTupleClient client = new ProxyTarantoolTupleClient(new ClusterTarantoolTupleClient(config, discoveryClientProvider));
        DefaultTarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(client.getConfig().getMessagePackMapper());
        return new TarantoolConnection(client, tupleFactory);
    }
}
