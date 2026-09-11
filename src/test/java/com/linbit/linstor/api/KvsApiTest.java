package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.KvsApi;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.event.EventSerializerDescriptor;
import com.linbit.linstor.event.WatchStore;
import com.linbit.linstor.event.serializer.EventSerializer;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class KvsApiTest extends ApiTestBase
{
    private static final String TEST_KVS_NAME = "TestKvs";

    /** modifyKvs always responds with a CTRL_CONF (not KVS) object mask. */
    private static final long RC_KVS_MODIFIED =
        ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.MODIFIED;

    @Inject
    private Provider<CtrlApiCallHandler> ctrlApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    // dependencies of the CtrlApiCallHandler facade that no test module provides
    @Bind
    @Mock
    protected WatchStore watchStore;

    @Bind
    protected Map<String, EventSerializer> eventSerializers = Collections.emptyMap();

    @Bind
    protected Map<String, EventSerializerDescriptor> eventSerializerDescriptors = Collections.emptyMap();

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        leaveScope();
    }

    @Test
    public void createKvsWithProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );

        KvsApi kvsApi = getSingleKvs();
        assertThat(kvsApi.getName()).isEqualTo(TEST_KVS_NAME);
        assertThat(kvsApi.getProps())
            .hasSize(1)
            .containsEntry("key1", "value1");
    }

    @Test
    public void modifyUpdatesAndAddsProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );
        // second round-trip: update the existing key, add a namespaced one
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "changed")
                .setProp("ns1/key2", "value2")
        );

        assertThat(getSingleKvs().getProps())
            .hasSize(2)
            .containsEntry("key1", "changed")
            .containsEntry("ns1/key2", "value2");
    }

    @Test
    public void deleteSinglePropKey() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
                .setProp("ns1/key2", "value2")
        );
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .deleteProp("key1")
        );

        assertThat(getSingleKvs().getProps())
            .hasSize(1)
            .containsEntry("ns1/key2", "value2");
    }

    @Test
    public void deleteNamespaceDeletesContainedProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
                .setProp("ns1/key2", "value2")
                .setProp("ns1/key3", "value3")
        );
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .deleteNamespace("ns1")
        );

        assertThat(getSingleKvs().getProps())
            .hasSize(1)
            .containsEntry("key1", "value1");
    }

    @Test
    public void removingLastPropDeletesKvs() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .deleteProp("key1")
        );

        // a KVS without properties is removed entirely
        assertThat(ctrlApiCallHandlerProvider.get().listKvs()).isEmpty();
        assertThat(kvsMap.get(new KeyValueStoreName(TEST_KVS_NAME))).isNull();
    }

    @Test
    public void createEmptyKvsIsNotPersisted() throws Exception
    {
        enterScope();
        // characterization: modifyKvs without any props creates the KVS but immediately
        // deletes it again since it has no properties - the call still reports success
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
        );

        assertThat(ctrlApiCallHandlerProvider.get().listKvs()).isEmpty();
    }

    @Test
    public void listMultipleKvs() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setKvsName("OtherKvs")
                .setProp("key2", "value2")
        );

        Set<KvsApi> kvsSet = ctrlApiCallHandlerProvider.get().listKvs();
        assertThat(kvsSet).hasSize(2);
        assertThat(kvsSet).extracting(KvsApi::getName)
            .containsExactlyInAnyOrder(TEST_KVS_NAME, "OtherKvs");
    }

    @Test
    public void modifyWithMatchingUuid() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );
        UUID kvsUuid = kvsMap.get(new KeyValueStoreName(TEST_KVS_NAME)).getUuid();

        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setKvsUuid(kvsUuid)
                .setProp("key2", "value2")
        );

        assertThat(getSingleKvs().getProps()).hasSize(2);
    }

    @Test
    public void modifyWithWrongUuidFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );

        Map<String, String> overrideProps = new TreeMap<>();
        overrideProps.put("key2", "value2");
        expectApiRcException(
            ApiConsts.FAIL_UUID_KVS,
            () -> ctrlApiCallHandlerProvider.get().modifyKvs(
                randomUUID(),
                TEST_KVS_NAME,
                overrideProps,
                Collections.emptySet(),
                Collections.emptySet()
            )
        );

        // the KVS was not changed
        assertThat(getSingleKvs().getProps())
            .hasSize(1)
            .containsEntry("key1", "value1");
    }

    @Test
    public void modifyWithInvalidNameFails() throws Exception
    {
        enterScope();
        Map<String, String> overrideProps = new TreeMap<>();
        overrideProps.put("key1", "value1");
        expectApiRcException(
            ApiConsts.FAIL_INVLD_KVS_NAME,
            () -> ctrlApiCallHandlerProvider.get().modifyKvs(
                null,
                "Invalid Name", // blank is not allowed
                overrideProps,
                Collections.emptySet(),
                Collections.emptySet()
            )
        );

        assertThat(ctrlApiCallHandlerProvider.get().listKvs()).isEmpty();
    }

    @Test
    public void modifyWithNullValueFails() throws Exception
    {
        enterScope();
        Map<String, String> overrideProps = new TreeMap<>();
        overrideProps.put("key1", null);
        expectApiRcException(
            ApiConsts.FAIL_INVLD_PROP,
            () -> ctrlApiCallHandlerProvider.get().modifyKvs(
                null,
                TEST_KVS_NAME,
                overrideProps,
                Collections.emptySet(),
                Collections.emptySet()
            )
        );
    }

    @Test
    public void deleteKvs() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );
        evaluateTest(
            new DeleteKvsCall(ApiConsts.DELETED)
        );

        assertThat(ctrlApiCallHandlerProvider.get().listKvs()).isEmpty();
        assertThat(kvsMap.get(new KeyValueStoreName(TEST_KVS_NAME))).isNull();
    }

    @Test
    public void deleteNonexistentKvsWarns() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteKvsCall(ApiConsts.WARN_NOT_FOUND)
        );
    }

    @Test
    public void deleteWithWrongUuidFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );

        expectApiRcException(
            ApiConsts.FAIL_UUID_KVS,
            () -> ctrlApiCallHandlerProvider.get().deleteKvs(randomUUID(), TEST_KVS_NAME)
        );

        assertThat(ctrlApiCallHandlerProvider.get().listKvs()).hasSize(1);
    }

    @Test
    public void deleteWithMatchingUuid() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyKvsCall(RC_KVS_MODIFIED)
                .setProp("key1", "value1")
        );
        UUID kvsUuid = kvsMap.get(new KeyValueStoreName(TEST_KVS_NAME)).getUuid();

        evaluateTest(
            new DeleteKvsCall(ApiConsts.DELETED)
                .setKvsUuid(kvsUuid)
        );

        assertThat(ctrlApiCallHandlerProvider.get().listKvs()).isEmpty();
    }

    private KvsApi getSingleKvs()
    {
        Set<KvsApi> kvsSet = ctrlApiCallHandlerProvider.get().listKvs();
        assertThat(kvsSet).hasSize(1);
        return kvsSet.iterator().next();
    }

    private void expectApiRcException(long expectedRc, org.assertj.core.api.ThrowableAssert.ThrowingCallable callable)
    {
        Throwable thrown = catchThrowable(callable);
        assertThat(thrown).isInstanceOf(ApiRcException.class);
        ApiRcException apiRcExc = (ApiRcException) thrown;
        assertThat(apiRcExc.getApiCallRc()).hasSize(1);
        assertThat(apiRcExc.getApiCallRc().get(0).getReturnCode()).isEqualTo(expectedRc);
    }

    /*
     * The kvs api call handler adds its entries without object / operation context, therefore
     * the testers are created with objMask and opMask 0 and fully specified expected return codes.
     */
    private class ModifyKvsCall extends AbsApiCallTester
    {
        private UUID kvsUuid;
        private String kvsName;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deleteNamespaces;

        ModifyKvsCall(long... expectedRcs)
        {
            super(0, 0, expectedRcs);
            kvsUuid = null;
            kvsName = TEST_KVS_NAME;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deleteNamespaces = new TreeSet<>();
        }

        ModifyKvsCall setKvsUuid(UUID uuid)
        {
            kvsUuid = uuid;
            return this;
        }

        ModifyKvsCall setKvsName(String kvsNameRef)
        {
            kvsName = kvsNameRef;
            return this;
        }

        ModifyKvsCall setProp(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyKvsCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyKvsCall deleteNamespace(String namespace)
        {
            deleteNamespaces.add(namespace);
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().modifyKvs(
                kvsUuid,
                kvsName,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            );
        }
    }

    private class DeleteKvsCall extends AbsApiCallTester
    {
        private UUID kvsUuid;
        private String kvsName;

        DeleteKvsCall(long... expectedRcs)
        {
            super(0, 0, expectedRcs);
            kvsUuid = null;
            kvsName = TEST_KVS_NAME;
        }

        DeleteKvsCall setKvsUuid(UUID uuid)
        {
            kvsUuid = uuid;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().deleteKvs(kvsUuid, kvsName);
        }
    }
}
