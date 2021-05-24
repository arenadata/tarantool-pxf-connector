package io.arenadata.pxf.plugins.tarantool.client;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;

public interface TarantoolConnectionProvider {
    TarantoolConnection provide(TarantoolClientConfig config,
                                TarantoolClusterAddressProvider discoveryClientProvider);
}
