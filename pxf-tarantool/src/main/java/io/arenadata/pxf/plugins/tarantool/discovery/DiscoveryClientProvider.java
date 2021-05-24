package io.arenadata.pxf.plugins.tarantool.discovery;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;

@FunctionalInterface
public interface DiscoveryClientProvider {
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> provide(TarantoolClientConfig config, TarantoolServerAddress routerAddress);
}
