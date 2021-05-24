package io.arenadata.pxf.plugins.tarantool.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public final class TarantoolConnection implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TarantoolConnection.class);

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private final TarantoolTupleFactory tupleFactory;

    public TarantoolConnection(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client,
                               TarantoolTupleFactory tupleFactory) {
        this.client = client;
        this.tupleFactory = tupleFactory;
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getClient() {
        return client;
    }

    public TarantoolTupleFactory getTupleFactory() {
        return tupleFactory;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            LOGGER.error("Exception during closing tarantool client, ignored", e);
        }
    }
}
