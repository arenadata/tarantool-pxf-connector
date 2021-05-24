package io.arenadata.pxf.plugins.tarantool.common;

import io.arenadata.pxf.plugins.tarantool.client.TarantoolConnection;
import io.arenadata.pxf.plugins.tarantool.client.TarantoolConnectionProvider;
import io.arenadata.pxf.plugins.tarantool.client.TarantoolConnectionProviderImpl;
import io.arenadata.pxf.plugins.tarantool.discovery.DiscoveryClientProvider;
import io.arenadata.pxf.plugins.tarantool.discovery.DiscoveryClusterAddressProvider;
import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies;
import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class TarantoolAccessorBase extends BasePlugin implements Accessor {
    private static final int DEFAULT_TIMEOUT_CONNECT = 5000;
    private static final int DEFAULT_TIMEOUT_READ = 5000;
    private static final int DEFAULT_TIMEOUT_REQUEST = 5000;

    private static final String TARANTOOL_SERVER = "tarantool.cartridge.server";
    private static final String USER = "tarantool.cartridge.user";
    private static final String PASSWORD = "tarantool.cartridge.password";
    private static final String TIMEOUT_CONNECT = "tarantool.cartridge.timeout.connect";
    private static final String TIMEOUT_READ = "tarantool.cartridge.timeout.read";
    private static final String TIMEOUT_REQUEST = "tarantool.cartridge.timeout.request";

    protected List<CompletableFuture<TarantoolResult<TarantoolTuple>>> futures = new ArrayList<>();
    protected String spaceName;
    protected TarantoolConnection connection;
    protected TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> spaceOperations;

    private DiscoveryClientProvider discoveryClientProvider = ClusterTarantoolTupleClient::new;
    private TarantoolConnectionProvider tarantoolConnectionProvider = new TarantoolConnectionProviderImpl();

    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
    private TarantoolCredentials credentials;
    private TarantoolServerAddress routerAddress;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        spaceName = requestContext.getDataSource();
        if (StringUtils.isBlank(spaceName)) {
            throw new IllegalArgumentException("Tarantool space must be set");
        }

        String serverHostPort = configuration.get(TARANTOOL_SERVER);
        if (StringUtils.isBlank(serverHostPort)) {
            throw new IllegalArgumentException("TARANTOOL_SERVER property must be set");
        }

        this.routerAddress = new TarantoolServerAddress(serverHostPort);

        String user = configuration.get(USER, "");
        String password = configuration.get(PASSWORD, "");
        if (!user.isEmpty()) {
            this.credentials = new SimpleTarantoolCredentials(user, password);
        } else {
            this.credentials = new SimpleTarantoolCredentials();
        }

        this.connectTimeout = configuration.getInt(TIMEOUT_CONNECT, DEFAULT_TIMEOUT_CONNECT);
        this.readTimeout = configuration.getInt(TIMEOUT_READ, DEFAULT_TIMEOUT_READ);
        this.requestTimeout = configuration.getInt(TIMEOUT_REQUEST, DEFAULT_TIMEOUT_REQUEST);
    }

    @Override
    public boolean openForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow readNextObject() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean openForWrite() throws Exception {
        LOG.info("Opening \"{}\" for write in {}. Segment: {}, total: {}",
                context.getProfile(), spaceName, context.getSegmentId(), context.getTotalSegments());
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout)
                .withRequestTimeout(requestTimeout)
                .withConnectionSelectionStrategyFactory(TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory.INSTANCE)
                .build();
        TarantoolClusterAddressProvider discoveryClusterAddressProvider = new DiscoveryClusterAddressProvider(config, routerAddress, discoveryClientProvider);
        connection = tarantoolConnectionProvider.provide(config, discoveryClusterAddressProvider);
        spaceOperations = connection.getClient().space(spaceName);
        futures.clear();
        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        LOG.info("Closing \"{}\" for write in \"{}\". Futures to check: {}, segment: {}, total: {}",
                context.getProfile(), spaceName, futures.size(), context.getSegmentId(), context.getTotalSegments());

        try {
            futures.forEach(future -> {
                try {
                    TarantoolResult<TarantoolTuple> result = future.get();
                    LOG.debug("Task completed successfully: {}", result);
                } catch (Exception e) {
                    LOG.error("Task ended up with exception", e);
                    throw new IllegalStateException("Exception during running task", e);
                }
            });

            LOG.info("Closing \"{}\" for write in \"{}\". All futures complete, segment: {}, total: {} ",
                    context.getProfile(), spaceName, context.getSegmentId(), context.getTotalSegments());
        } finally {
            futures.clear();
            if (connection != null) {
                connection.close();
            }
            LOG.info("Closed \"{}\" for write in \"{}\", segment: {}, total: {}",
                    context.getProfile(), spaceName, context.getSegmentId(), context.getTotalSegments());
        }
    }

    public void setDiscoveryClientProvider(DiscoveryClientProvider discoveryClientProvider) {
        this.discoveryClientProvider = discoveryClientProvider;
    }

    public void setTarantoolConnectionProvider(TarantoolConnectionProvider tarantoolConnectionProvider) {
        this.tarantoolConnectionProvider = tarantoolConnectionProvider;
    }
}
