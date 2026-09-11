package com.linbit.linstor.api;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlConfApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.ControllerConfigApi;
import com.linbit.linstor.core.cfg.CtrlConfig;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class CtrlConfApiTest extends ApiTestBase
{
    private static final String KEY_KO_COUNT = ApiConsts.NAMESPC_DRBD_OPTIONS + "/Net/ko-count";
    private static final String KEY_AUTO_EVICT_AFTER_TIME =
        ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_AFTER_TIME;

    @Inject
    private Provider<CtrlConfApiCallHandler> ctrlConfApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Inject
    private CtrlConfig ctrlCfg;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        leaveScope();
    }

    @Test
    public void setWhitelistedCtrlProp() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(ApiConsts.KEY_SEARCH_DOMAIN, "example.com")
        );
        assertThat(ctrlConf.getProp(ApiConsts.KEY_SEARCH_DOMAIN)).isEqualTo("example.com");
        assertThat(ctrlConfApiCallHandlerProvider.get().listProps())
            .containsEntry(ApiConsts.KEY_SEARCH_DOMAIN, "example.com");
    }

    @Test
    public void setWhitelistedStltProp() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(KEY_KO_COUNT, "10")
        );
        assertThat(stltConf.getProp(KEY_KO_COUNT)).isEqualTo("10");
    }

    @Test
    public void setNonWhitelistedPropFails() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.FAIL_INVLD_PROP)
                .setProp("ThisIsNotAWhitelistedKey", "value")
        );
        assertThat(ctrlConf.getProp("ThisIsNotAWhitelistedKey")).isNull();
        assertThat(stltConf.getProp("ThisIsNotAWhitelistedKey")).isNull();
    }

    @Test
    public void setInvalidValueForWhitelistedPropFails() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.FAIL_INVLD_PROP)
                .setProp(KEY_AUTO_EVICT_AFTER_TIME, "NotANumber")
        );
        assertThat(ctrlConf.getProp(KEY_AUTO_EVICT_AFTER_TIME)).isNull();
    }

    @Test
    public void setDeprecatedExtCmdWaitTimeout() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(
                ApiConsts.WARN_DEPRECATED,
                ApiConsts.MASK_CRT | ApiConsts.CREATED
            )
                .setProp(ApiConsts.KEY_EXT_CMD_WAIT_TO, "5000")
        );
        // the deprecated key is re-mapped to the ExtCmd namespace
        assertThat(stltConf.getProp(ApiConsts.NAMESPC_EXT_CMD + "/" + ApiConsts.KEY_WAIT_TO)).isEqualTo("5000");
    }

    @Test
    public void deleteCtrlProp() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(ApiConsts.KEY_SEARCH_DOMAIN, "example.com")
        );
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_DEL | ApiConsts.DELETED)
                .deleteProp(ApiConsts.KEY_SEARCH_DOMAIN)
        );
        assertThat(ctrlConf.getProp(ApiConsts.KEY_SEARCH_DOMAIN)).isNull();
    }

    @Test
    public void deleteNonWhitelistedPropFails() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_DEL | ApiConsts.FAIL_INVLD_PROP)
                .deleteProp("ThisIsNotAWhitelistedKey")
        );
    }

    @Test
    public void deleteNamespaceDeletesContainedProps() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(KEY_AUTO_EVICT_AFTER_TIME, "100")
        );
        assertThat(ctrlConf.getProp(KEY_AUTO_EVICT_AFTER_TIME)).isEqualTo("100");

        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_DEL | ApiConsts.DELETED)
                .deleteNamespace(ApiConsts.NAMESPC_DRBD_OPTIONS)
        );
        assertThat(ctrlConf.getProp(KEY_AUTO_EVICT_AFTER_TIME)).isNull();
        assertThat(ctrlConf.getNamespace(ApiConsts.NAMESPC_DRBD_OPTIONS)).isNull();
    }

    @Test
    public void setTcpPortAutoRange() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(
                ApiConsts.MASK_MOD | ApiConsts.MODIFIED,
                ApiConsts.MASK_CRT | ApiConsts.CREATED
            )
                .setProp(ApiConsts.KEY_TCP_PORT_AUTO_RANGE, "7000-7999")
        );
        assertThat(ctrlConf.getProp(ApiConsts.KEY_TCP_PORT_AUTO_RANGE)).isEqualTo("7000-7999");
    }

    @Test
    public void setTcpPortAutoRangeOutOfBoundsFails() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.FAIL_INVLD_TCP_PORT)
                .setProp(ApiConsts.KEY_TCP_PORT_AUTO_RANGE, "65000-70000")
        );
        assertThat(ctrlConf.getProp(ApiConsts.KEY_TCP_PORT_AUTO_RANGE)).isNull();
    }

    @Test
    public void setMinorNrAutoRange() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(
                ApiConsts.MASK_MOD | ApiConsts.MODIFIED,
                ApiConsts.MASK_CRT | ApiConsts.CREATED
            )
                .setProp(ApiConsts.KEY_MINOR_NR_AUTO_RANGE, "10000-49999")
        );
        assertThat(ctrlConf.getProp(ApiConsts.KEY_MINOR_NR_AUTO_RANGE)).isEqualTo("10000-49999");
        Mockito.verify(minorNrPoolMock).reloadRange();
    }

    @Test
    public void setMinorNrAutoRangeOutOfBoundsFails() throws Exception
    {
        // besides the failure entry, setProp currently also adds its generic "successfully set" entry,
        // but the transaction is rolled back and the property is not persisted
        evaluateTest(
            new ModifyCtrlCall(
                ApiConsts.FAIL_INVLD_MINOR_NR,
                ApiConsts.MASK_CRT | ApiConsts.CREATED
            )
                .setProp(ApiConsts.KEY_MINOR_NR_AUTO_RANGE, "0-99999999")
        );
        assertThat(ctrlConf.getProp(ApiConsts.KEY_MINOR_NR_AUTO_RANGE)).isNull();
    }

    @Test
    public void listPropsMergesCtrlAndStltProps() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(ApiConsts.KEY_SEARCH_DOMAIN, "example.com")
        );
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(KEY_KO_COUNT, "10")
        );

        Map<String, String> props = ctrlConfApiCallHandlerProvider.get().listProps();
        assertThat(props)
            .containsEntry(ApiConsts.KEY_SEARCH_DOMAIN, "example.com") // controller prop
            .containsEntry(KEY_KO_COUNT, "10"); // satellite prop
    }

    @Test
    public void deletePropWithCommit() throws Exception
    {
        evaluateTest(
            new ModifyCtrlCall(ApiConsts.MASK_CRT | ApiConsts.CREATED)
                .setProp(ApiConsts.KEY_SEARCH_DOMAIN, "example.com")
        );

        ApiCallRc rc = collect(
            ctrlConfApiCallHandlerProvider.get().deletePropWithCommit(ApiConsts.KEY_SEARCH_DOMAIN, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_DEL | ApiConsts.DELETED, rc.get(0));
        assertThat(ctrlConf.getProp(ApiConsts.KEY_SEARCH_DOMAIN)).isNull();
    }

    @Test
    public void isValidTcpPortBoundaries()
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        assertThat(CtrlConfApiCallHandler.isValidTcpPort(1, apiCallRc)).isTrue();
        assertThat(CtrlConfApiCallHandler.isValidTcpPort(65535, apiCallRc)).isTrue();
        assertThat(apiCallRc).isEmpty();

        assertThat(CtrlConfApiCallHandler.isValidTcpPort(0, apiCallRc)).isFalse();
        assertThat(apiCallRc).hasSize(1);
        assertThat(apiCallRc.get(0).getReturnCode()).isEqualTo(ApiConsts.FAIL_INVLD_TCP_PORT);

        assertThat(CtrlConfApiCallHandler.isValidTcpPort(65536, apiCallRc)).isFalse();
        assertThat(apiCallRc).hasSize(2);
        assertThat(apiCallRc.get(1).getReturnCode()).isEqualTo(ApiConsts.FAIL_INVLD_TCP_PORT);
    }

    @Test
    public void setCtrlConfigLogLevel() throws Exception
    {
        // use the levels the test error reporter is initialized with to not affect other tests
        ApiCallRc rc = collect(
            ctrlConfApiCallHandlerProvider.get().setCtrlConfig(new LogLevelConfig("info", "trace", null, null))
        );
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.MODIFIED, rc.get(0));
        assertThat(ctrlCfg.getLogLevel()).isEqualTo("info");
        assertThat(ctrlCfg.getLogLevelLinstor()).isEqualTo("trace");
    }

    @Test
    public void setCtrlConfigInvalidLogLevelFails() throws Exception
    {
        try
        {
            collect(
                ctrlConfApiCallHandlerProvider.get().setCtrlConfig(new LogLevelConfig("bogus", null, null, null))
            );
            fail("Expected ApiRcException for invalid log level");
        }
        catch (ApiRcException exc)
        {
            assertThat(exc.getApiCallRc()).hasSize(1);
            assertThat(exc.getApiCallRc().get(0).getReturnCode()).isEqualTo(ApiConsts.FAIL_INVLD_CONF);
        }
    }

    @Test
    public void masterPassphraseInitiallyUnset()
    {
        assertThat(ctrlConfApiCallHandlerProvider.get().masterPassphraseStatus())
            .isEqualTo(CtrlConfApiCallHandler.LinstorEncryptionStatus.UNSET);
    }

    @Test
    public void enterPassphraseWithoutPassphraseSetFails() throws Exception
    {
        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().enterPassphrase("noPassphraseSetYet"));
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.FAIL_MISSING_PROPS, rc.get(0));
    }

    @Test
    public void createMasterPassphrase() throws Exception
    {
        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("topSecret", null));
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.CREATED, rc.get(0));

        assertThat(ctrlConfApiCallHandlerProvider.get().masterPassphraseStatus())
            .isEqualTo(CtrlConfApiCallHandler.LinstorEncryptionStatus.UNLOCKED);
    }

    @Test
    public void createMasterPassphraseTwiceFails() throws Exception
    {
        collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("topSecret", null));

        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("otherSecret", null));
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
            rc.get(0)
        );
    }

    @Test
    public void modifyMasterPassphrase() throws Exception
    {
        collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("topSecret", null));

        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("newSecret", "topSecret"));
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.MODIFIED, rc.get(0));

        // the new passphrase must now be accepted
        ApiCallRc enterRc = collect(ctrlConfApiCallHandlerProvider.get().enterPassphrase("newSecret"));
        assertThat(enterRc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.PASSPHRASE_ACCEPTED,
            enterRc.get(0)
        );
    }

    @Test
    public void modifyMasterPassphraseWithWrongOldPassphraseFails() throws Exception
    {
        collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("topSecret", null));

        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("newSecret", "wrongOldSecret"));
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.FAIL_MISSING_PROPS, rc.get(0));
    }

    @Test
    public void enterWrongPassphraseFails() throws Exception
    {
        collect(ctrlConfApiCallHandlerProvider.get().setPassphrase("topSecret", null));

        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().enterPassphrase("wrongSecret"));
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.FAIL_MISSING_PROPS, rc.get(0));
    }

    private static class LogLevelConfig implements ControllerConfigApi
    {
        private final @Nullable String logLevel;
        private final @Nullable String logLevelLinstor;
        private final @Nullable String logLevelGlobal;
        private final @Nullable String logLevelLinstorGlobal;

        LogLevelConfig(
            @Nullable String logLevelRef,
            @Nullable String logLevelLinstorRef,
            @Nullable String logLevelGlobalRef,
            @Nullable String logLevelLinstorGlobalRef
        )
        {
            logLevel = logLevelRef;
            logLevelLinstor = logLevelLinstorRef;
            logLevelGlobal = logLevelGlobalRef;
            logLevelLinstorGlobal = logLevelLinstorGlobalRef;
        }

        @Override
        public @Nullable String getLogLevel()
        {
            return logLevel;
        }

        @Override
        public @Nullable String getLogLevelLinstor()
        {
            return logLevelLinstor;
        }

        @Override
        public @Nullable String getLogLevelGlobal()
        {
            return logLevelGlobal;
        }

        @Override
        public @Nullable String getLogLevelLinstorGlobal()
        {
            return logLevelLinstorGlobal;
        }
    }

    private class ModifyCtrlCall extends AbsApiCallTester
    {
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deletePropNamespaces;

        ModifyCtrlCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_CTRL_CONF,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deletePropNamespaces = new TreeSet<>();
        }

        ModifyCtrlCall setProp(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyCtrlCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyCtrlCall deleteNamespace(String namespace)
        {
            deletePropNamespaces.add(namespace);
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            ctrlConfApiCallHandlerProvider.get().modifyCtrl(
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            )
                .contextWrite(contextWrite())
                .toStream().forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
