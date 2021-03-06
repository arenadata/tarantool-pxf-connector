/**
 * Copyright © 2021 tarantool-pxf-connector
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.pxf.plugins.tarantool.delete;

import io.arenadata.pxf.plugins.tarantool.client.TarantoolConnection;
import io.arenadata.pxf.plugins.tarantool.client.TarantoolConnectionProvider;
import io.arenadata.pxf.plugins.tarantool.discovery.DiscoveryClientProvider;
import io.tarantool.driver.DefaultTarantoolTupleFactory;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolTupleResult;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TarantoolAccessorTest {
    private static final String USERNAME = "someuser";
    private static final String VALID_HOST = "localhost:1111";
    private static final String NOT_VALID_HOST = "loca:lhost:1111";
    private static final String PASSWORD = "123";
    private static final String TIMEOUT_CONNECT = "4000";
    private static final String TIMEOUT_READ = "5000";
    private static final String TIMEOUT_REQUEST = "6000";
    private static final String SPACE = "space";

    @Mock
    private TarantoolConnectionProvider tarantoolConnectionProvider;

    @Mock
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientDiscovery;

    @Mock
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientOperations;

    @Mock
    private TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> spaceOperations;

    @Mock
    private TarantoolMetadataOperations tarantoolMetadataOperations;

    @Mock
    private DiscoveryClientProvider discoveryClientProvider;

    @Captor
    private ArgumentCaptor<TarantoolClientConfig> configArgumentCaptor;

    @Captor
    private ArgumentCaptor<TarantoolServerAddress> tarantoolServerAddressArgumentCaptor;

    private RequestContext context;

    private TarantoolAccessor tarantoolDeleteAccessor;

    @BeforeEach
    void setUp() {
        context = new RequestContext();
        context.setSegmentId(0);
        context.setTotalSegments(1);
        context.setUser("user");
        context.setConfig("default");
        context.setDataSource(SPACE);
        context.setTupleDescription(Arrays.asList(
                new ColumnDescriptor("id", DataType.BIGINT.getOID(), 0, null, null),
                new ColumnDescriptor("name", DataType.VARCHAR.getOID(), 0, null, null),
                new ColumnDescriptor("bucket_id", DataType.BIGINT.getOID(), 0, null, null)));
        context.setAdditionalConfigProps(new HashMap<String, String>() {{
            put("tarantool.cartridge.server", VALID_HOST);
            put("tarantool.cartridge.user", USERNAME);
            put("tarantool.cartridge.password", PASSWORD);
            put("tarantool.cartridge.timeout.connect", TIMEOUT_CONNECT);
            put("tarantool.cartridge.timeout.read", TIMEOUT_READ);
            put("tarantool.cartridge.timeout.request", TIMEOUT_REQUEST);
        }});


        Map<String, Map<String, String>> discoveryResult = new HashMap<>();
        HashMap<String, String> address1 = new HashMap<>();
        address1.put("uri", "localhost:1111");
        discoveryResult.put("key1", address1);
        lenient().when(clientDiscovery.eval(Mockito.anyString())).thenReturn(CompletableFuture.completedFuture(asList(discoveryResult)));

        DefaultMessagePackMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        lenient().when(tarantoolConnectionProvider.provide(any(), any())).thenAnswer(invocation -> {
            ((TarantoolClusterAddressProvider) invocation.getArgument(1)).getAddresses(); //hack to emulate discovery
            return new TarantoolConnection(clientOperations, new DefaultTarantoolTupleFactory(mapper));
        });
        lenient().when(clientOperations.space(Mockito.eq(SPACE))).thenReturn(spaceOperations);

        lenient().when(discoveryClientProvider.provide(any(), any())).thenAnswer(invocation -> clientDiscovery);
        lenient().when(clientOperations.metadata()).thenReturn(tarantoolMetadataOperations);

        TarantoolIndexMetadata primaryIndexMetadata = new TarantoolIndexMetadata();
        primaryIndexMetadata.setIndexParts(Arrays.asList(new TarantoolIndexPartMetadata(0, "integer", "id"),
                new TarantoolIndexPartMetadata(1, "string", "name"),
                new TarantoolIndexPartMetadata(2, "integer", "bucket_id")));
        lenient().when(tarantoolMetadataOperations.getIndexById(Mockito.eq(SPACE), Mockito.eq(0))).thenReturn(Optional.of(primaryIndexMetadata));
        lenient().when(spaceOperations.delete(Mockito.any())).thenReturn(CompletableFuture.completedFuture(Mockito.mock(TarantoolTupleResult.class)));

        tarantoolDeleteAccessor = new TarantoolAccessor();
        tarantoolDeleteAccessor.setTarantoolConnectionProvider(tarantoolConnectionProvider);
        tarantoolDeleteAccessor.setDiscoveryClientProvider(discoveryClientProvider);
    }

    @Test
    void shouldFailWhenNoSpaceDefined() {
        // arrange
        context.setDataSource("");

        // act assert
        assertThrows(IllegalArgumentException.class, () -> tarantoolDeleteAccessor.initialize(context));
    }

    @Test
    void shouldFailWhenNoHostDefined() {
        // arrange
        context.getAdditionalConfigProps().remove("tarantool.cartridge.server");

        // act assert
        assertThrows(IllegalArgumentException.class, () -> tarantoolDeleteAccessor.initialize(context));
    }

    @Test
    void shouldFailWhenNoHostNotValid() {
        // arrange
        context.getAdditionalConfigProps().put("tarantool.cartridge.server", NOT_VALID_HOST);

        // act assert
        assertThrows(IllegalArgumentException.class, () -> tarantoolDeleteAccessor.initialize(context));
    }

    @Test
    void shouldFailWhenNoPrimaryIndexOnTarantool() throws Exception {
        // arrange
        reset(tarantoolMetadataOperations);
        when(tarantoolMetadataOperations.getIndexById(Mockito.eq(SPACE), Mockito.eq(0))).thenReturn(Optional.empty());

        // act assert
        tarantoolDeleteAccessor.initialize(context);
        assertThrows(IllegalArgumentException.class, () -> tarantoolDeleteAccessor.openForWrite());
    }

    @Test
    void shouldFailWhenColumnsSizeDiffer() {
        // arrange
        reset(tarantoolMetadataOperations);
        TarantoolIndexMetadata primaryIndexMetadata = new TarantoolIndexMetadata();
        primaryIndexMetadata.setIndexParts(Arrays.asList(new TarantoolIndexPartMetadata(0, "integer", "id"),
                new TarantoolIndexPartMetadata(2, "integer", "bucket_id")));
        lenient().when(tarantoolMetadataOperations.getIndexById(Mockito.eq(SPACE), Mockito.eq(0))).thenReturn(Optional.of(primaryIndexMetadata));

        // act assert
        tarantoolDeleteAccessor.initialize(context);
        assertThrows(IllegalArgumentException.class, () -> tarantoolDeleteAccessor.openForWrite());
    }

    @Test
    void shouldFailWhenColumnsNameDiffer() {
        // arrange
        reset(tarantoolMetadataOperations);
        TarantoolIndexMetadata primaryIndexMetadata = new TarantoolIndexMetadata();
        primaryIndexMetadata.setIndexParts(Arrays.asList(new TarantoolIndexPartMetadata(0, "integer", "id"),
                new TarantoolIndexPartMetadata(1, "string", "wrong"),
                new TarantoolIndexPartMetadata(2, "integer", "bucket_id")));
        lenient().when(tarantoolMetadataOperations.getIndexById(Mockito.eq(SPACE), Mockito.eq(0))).thenReturn(Optional.of(primaryIndexMetadata));

        // act assert
        tarantoolDeleteAccessor.initialize(context);
        assertThrows(IllegalArgumentException.class, () -> tarantoolDeleteAccessor.openForWrite());
    }

    @Test
    void shouldCorrectlyProcess() throws Exception {
        // act
        tarantoolDeleteAccessor.initialize(context);
        assertTrue(tarantoolDeleteAccessor.openForWrite());
        assertTrue(tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(1L, "test", 1))));
        assertTrue(tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(2L, "test2", 2))));
        tarantoolDeleteAccessor.closeForWrite();

        // assert
        verify(clientOperations).space(Mockito.eq(SPACE));
        verify(clientOperations).metadata();
        verify(clientOperations).close();
        verifyNoMoreInteractions(clientOperations);

        verify(spaceOperations, times(2)).delete(Mockito.any());
        verifyNoMoreInteractions(spaceOperations);
    }

    @Test
    void shouldReturnFalseWhenExceptionDuringRequest() throws Exception {
        // arrange
        when(spaceOperations.delete(Mockito.any())).thenThrow(new RuntimeException("Exception"));

        // act
        tarantoolDeleteAccessor.initialize(context);
        tarantoolDeleteAccessor.openForWrite();
        assertFalse(tarantoolDeleteAccessor.writeNextObject(new OneRow(asList(1L, "test", 1))));
    }

    @Test
    void shouldThrowExceptionWhenFutureFailed() throws Exception {
        // arrange
        CompletableFuture<TarantoolResult<TarantoolTuple>> value = new CompletableFuture<>();
        value.completeExceptionally(new RuntimeException("Exception"));
        when(spaceOperations.delete(Mockito.any())).thenReturn(value);

        // act
        tarantoolDeleteAccessor.initialize(context);
        tarantoolDeleteAccessor.openForWrite();
        tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(1L, "test", 1)));
        tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(2L, "test2", 2)));
        assertThrows(IllegalStateException.class, () -> tarantoolDeleteAccessor.closeForWrite());

        // assert
        verify(clientOperations).space(Mockito.eq(SPACE));
        verify(clientOperations).metadata();
        verify(clientOperations).close();
        verifyNoMoreInteractions(clientOperations);

        verify(spaceOperations, times(2)).delete(Mockito.any());
        verifyNoMoreInteractions(spaceOperations);
    }

    @Test
    void validateConfigOfDiscovery() throws Exception {
        // act
        tarantoolDeleteAccessor.initialize(context);
        tarantoolDeleteAccessor.openForWrite();
        tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(1L, "test", 1)));
        tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(2L, "test2", 2)));
        tarantoolDeleteAccessor.closeForWrite();

        // assert
        verify(discoveryClientProvider).provide(configArgumentCaptor.capture(), tarantoolServerAddressArgumentCaptor.capture());

        TarantoolClientConfig config = configArgumentCaptor.getValue();
        assertEquals(USERNAME, config.getCredentials().getUsername());
        assertEquals(PASSWORD, ((SimpleTarantoolCredentials) config.getCredentials()).getPassword());
        assertEquals(Integer.parseInt(TIMEOUT_CONNECT), config.getConnectTimeout());
        assertEquals(Integer.parseInt(TIMEOUT_READ), config.getReadTimeout());
        assertEquals(Integer.parseInt(TIMEOUT_REQUEST), config.getRequestTimeout());

        TarantoolServerAddress serverAddress = tarantoolServerAddressArgumentCaptor.getValue();
        assertEquals("localhost", serverAddress.getHost());
        assertEquals(1111, serverAddress.getPort());
    }

    @Test
    void shouldPutGuestWhenNoUserDefined() throws Exception {
        // arrange
        context.getAdditionalConfigProps().remove("tarantool.cartridge.user");

        // act
        tarantoolDeleteAccessor.initialize(context);
        tarantoolDeleteAccessor.openForWrite();
        tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(1L, "test", 1)));
        tarantoolDeleteAccessor.writeNextObject(new OneRow(Arrays.asList(2L, "test2", 2)));
        tarantoolDeleteAccessor.closeForWrite();

        // assert
        verify(discoveryClientProvider).provide(configArgumentCaptor.capture(), tarantoolServerAddressArgumentCaptor.capture());

        TarantoolClientConfig config = configArgumentCaptor.getValue();
        assertEquals("guest", config.getCredentials().getUsername());
        assertEquals("", ((SimpleTarantoolCredentials) config.getCredentials()).getPassword());
    }
}