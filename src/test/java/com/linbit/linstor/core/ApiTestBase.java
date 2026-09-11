package com.linbit.linstor.core;

import com.linbit.ServiceName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandlerModule;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.ControllerSQLTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class ApiTestBase extends GenericDbBase
{
    @Mock
    protected TcpConnector tcpConnectorMock;

    @Bind @Mock
    protected NetComContainer netComContainer;

    @Bind
    protected Scheduler scheduler = VirtualTimeScheduler.create();

    @Inject @Named(LinStor.CONTROLLER_PROPS)
    protected Props ctrlConf;

    @Inject @Named(LinStor.SATELLITE_PROPS)
    protected Props stltConf;

    @Before
    @SuppressWarnings("checkstyle:variabledeclarationusagedistance")
    public void setUp() throws Exception
    {
        // SatelliteConnectorImpl stltConnector = Mockito.mock(SatelliteConnectorImpl.class);
        super.setUpWithoutEnteringScope(new CtrlApiCallHandlerModule());

        try (LinStorScope.ScopeAutoCloseable close = testScope.enter())
        {
            TransactionMgrSQL transMgr = new ControllerSQLTransactionMgr(dbConnPool);
            testScope.seed(TransactionMgr.class, transMgr);
            testScope.seed(TransactionMgrSQL.class, transMgr);

            ctrlConf.setConnection(transMgr);
            ctrlConf.setProp(ControllerNetComInitializer.PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC, "ignore");
            ctrlConf.setProp(ControllerNetComInitializer.PROPSCON_KEY_DEFAULT_SSL_CON_SVC, "ignore");

            transMgr.commit();
            transMgr.returnConnection();
        }

        Mockito.when(netComContainer.getNetComConnector(Mockito.any(ServiceName.class)))
            .thenReturn(tcpConnectorMock);

        enterScope();
        initAfterScopeWasEntered();
    }

    protected Context contextWrite()
    {
        return Context.of(
            ApiModule.API_CALL_NAME, "TestApiCallName",
            Peer.class, mockPeer,
            ApiModule.API_CALL_ID, 1L
        );
    }

    /**
     * Subscribes to the given flux (with the usual test api call context) and collects all emitted
     * ApiCallRc entries into a single ApiCallRcImpl.
     */
    protected ApiCallRcImpl collect(Flux<ApiCallRc> flux)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        flux
            .contextWrite(contextWrite())
            .toStream()
            .forEach(apiCallRc::addEntries);
        return apiCallRc;
    }

    /**
     * Stubs the usual satellite behavior on the given peer mock: satellite state (and its lock)
     * are present, api calls complete with an empty flux and the given ExtToolsManager (usually
     * also a mock, see {@link #stubAllExtToolsSupported}) is returned.
     */
    protected void stubSatellitePeer(
        Peer peerMock,
        ExtToolsManager extToolsMgrRef,
        SatelliteState stltStateRef,
        boolean online
    )
    {
        Mockito.when(peerMock.getExtToolsManager()).thenReturn(extToolsMgrRef);
        Mockito.when(peerMock.getSatelliteStateLock()).thenReturn(new ReentrantReadWriteLock());
        Mockito.when(peerMock.getSatelliteState()).thenReturn(stltStateRef);
        Mockito.when(peerMock.apiCall(Mockito.anyString(), Mockito.any())).thenReturn(Flux.empty());
        setSatelliteOnline(peerMock, online);
    }

    protected void setSatelliteOnline(Peer peerMock, boolean online)
    {
        Mockito.when(peerMock.isOnline()).thenReturn(online);
        Mockito.when(peerMock.getConnectionStatus()).thenReturn(
            online ? ApiConsts.ConnectionStatus.ONLINE : ApiConsts.ConnectionStatus.OFFLINE
        );
    }

    /**
     * Stubs the given ExtToolsManager mock to support all device layers and providers.
     */
    protected void stubAllExtToolsSupported(ExtToolsManager extToolsMgrMock)
    {
        Mockito.when(extToolsMgrMock.getSupportedLayers())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceLayerKind.values())));
        Mockito.when(extToolsMgrMock.getSupportedProviders())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceProviderKind.values())));
        Mockito.when(extToolsMgrMock.isLayerSupported(Mockito.any())).thenReturn(true);
        Mockito.when(extToolsMgrMock.isProviderSupported(Mockito.any())).thenReturn(true);
    }

    protected static NetInterfaceApi createNetInterfaceApi(String name, String address)
    {
        return createNetInterfaceApi(
            name,
            address,
            ApiConsts.DFLT_STLT_PORT_PLAIN,
            EncryptionType.PLAIN.name()
        );
    }

    protected static NetInterfaceApi createNetInterfaceApi(
        String name,
        String address,
        Integer port,
        String encrType
    )
    {
        return createNetInterfaceApi(java.util.UUID.randomUUID(), name, address, port, encrType);
    }

    protected static NetInterfaceApi createNetInterfaceApi(
        java.util.UUID uuid,
        String name,
        String address,
        Integer port,
        String encrType
    )
    {
        return new NetInterfacePojo(uuid, name, address, port, encrType);
    }

    protected void expectRc(long index, long expectedRc, RcEntry rcEntry)
    {
        if (rcEntry.getReturnCode() != expectedRc)
        {
            Assert.fail("Expected [" + index + "] RC to be " +
                resolveRC(expectedRc) + " but got " +
                resolveRC(rcEntry.getReturnCode())
            );
        }
    }

    private String resolveRC(long expectedRc)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        ApiRcUtils.appendReadableRetCode(sb, expectedRc);
        sb.append("]");
        return sb.toString();
    }

    protected RcEntry checkedGet(ApiCallRc rc, int idx)
    {
        assertThat(rc.size()).isGreaterThanOrEqualTo(idx + 1);

        return rc.get(idx);
    }

    protected RcEntry checkedGet(ApiCallRc rc, int idx, int expectedSize)
    {
        assertThat(expectedSize).isGreaterThan(idx);
        assertThat(rc).hasSize(expectedSize);

        return rc.get(idx);
    }

    protected void evaluateTest(AbsApiCallTester currentCall) throws Exception
    {
        evaluateTest(currentCall, true);
    }

    protected void evaluateTest(
        AbsApiCallTester currentCall,
        boolean checkReturnCodes
    )
        throws Exception
    {
        Mockito.reset(satelliteConnector);

        ApiCallRc rc = currentCall.executeApiCall();

        if (checkReturnCodes)
        {
            List<Long> expectedRetCodes = currentCall.retCodes;

            assertThat(rc).hasSameSizeAs(expectedRetCodes);
            for (int idx = 0; idx < expectedRetCodes.size(); idx++)
            {
                expectRc(idx, expectedRetCodes.get(idx), rc.get(idx));
            }
        }

        Mockito.verify(satelliteConnector, Mockito.times(currentCall.expectedSyncConnectingAttempts.size()))
            .startConnecting(
                Mockito.any(Node.class),
                Mockito.eq(false),
                Mockito.any(Object.class)
            );
        Mockito.verify(satelliteConnector, Mockito.times(currentCall.expectedAsyncConnectingAttempts.size()))
            .startConnecting(
                Mockito.any(Node.class),
                Mockito.eq(true),
                Mockito.any(Object.class)
            );
    }
}
