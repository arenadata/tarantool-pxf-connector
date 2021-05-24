package io.arenadata.pxf.plugins.tarantool.discovery;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DiscoveryClusterAddressProvider implements TarantoolClusterAddressProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryClusterAddressProvider.class);
    private static final String DISCOVERY_COMMAND = "local cartridge = require('cartridge')\n" +
            "local function table_contains(table, element)\n" +
            "    for _, value in pairs(table) do\n" +
            "        if value == element then\n" +
            "            return true\n" +
            "        end\n" +
            "    end\n" +
            "    return false\n" +
            "end\n" +
            "\n" +
            "local servers, err = cartridge.admin_get_servers()\n" +
            "local routers = {}\n" +
            "\n" +
            "for _, server in pairs(servers) do\n" +
            "    if server.replicaset ~= nil then\n" +
            "        if table_contains(server.replicaset.roles, 'crud-router') then\n" +
            "            routers[server.uuid] = {\n" +
            "                status = server.status,\n" +
            "                uuid = server.uuid,\n" +
            "                uri = server.uri,\n" +
            "                priority = server.priority\n" +
            "            }\n" +
            "        end\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "return routers";
    private final TarantoolClientConfig config;
    private final TarantoolServerAddress routerAddress;
    private final DiscoveryClientProvider clientProvider;

    public DiscoveryClusterAddressProvider(TarantoolClientConfig config, TarantoolServerAddress routerAddress, DiscoveryClientProvider clientProvider) {
        this.config = config;
        this.routerAddress = routerAddress;
        this.clientProvider = clientProvider;
    }

    @Override
    public synchronized Collection<TarantoolServerAddress> getAddresses() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = null;
        try {
            List<?> result;
            try {
                client = clientProvider.provide(config, routerAddress);
                CompletableFuture<List<?>> resultFuture = client.eval(DISCOVERY_COMMAND);
                result = resultFuture.get();
            } catch (Exception e) {
                LOGGER.error("Unexpected exception during discovery", e);
                throw new TarantoolClientException("Unexpected exception during discovery", e);
            }

            if (result.size() != 1) {
                LOGGER.error("Unexpected result of discovery call, expected: 1, got: {}", result.size());
                throw new TarantoolClientException("Incorrect result of discovery call, expected: 1, got: " + result.size());
            }

            Map<String, Map<String, String>> foundServices = (Map<String, Map<String, String>>) result.get(0);
            if (foundServices.isEmpty()) {
                LOGGER.error("Could not discover servers. Result is empty.");
                throw new TarantoolClientException("Could not discover servers. Result is empty.");
            }

            List<TarantoolServerAddress> tarantoolServerAddresses = new ArrayList<>();
            try {
                for (Map<String, String> value : foundServices.values()) {
                    String uri = value.get("uri");
                    tarantoolServerAddresses.add(new TarantoolServerAddress(uri));
                }
            } catch (Exception e) {
                LOGGER.error("Exception during parsing discovery result", e);
                throw new TarantoolClientException("Exception during parsing discovery result", e);
            }

            LOGGER.info("Successfully retrieved tarantool servers: {}", tarantoolServerAddresses);
            return tarantoolServerAddresses;
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    LOGGER.error("Exception during discovery tarantool client close, ignored", e);
                }
            }
        }
    }

    @Override
    public void close() {
        // no op
    }
}
