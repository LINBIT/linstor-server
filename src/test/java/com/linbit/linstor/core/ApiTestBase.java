package com.linbit.linstor.core;

import com.linbit.ServiceName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandlerModule;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.SecurityType;
import com.linbit.linstor.transaction.manager.ControllerSQLTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.List;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
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

            create(transMgr, ALICE_ACC_CTX);
            create(transMgr, BOB_ACC_CTX);

            transMgr.commit();
            transMgr.returnConnection();
        }

        Mockito.when(netComContainer.getNetComConnector(Mockito.any(ServiceName.class)))
            .thenReturn(tcpConnectorMock);

        enterScope();
        initAfterScopeWasEntered();
    }

    private void create(TransactionMgrSQL transMgr, AccessContext accCtx) throws Exception
    {
        Identity.create(SYS_CTX, accCtx.subjectId.name);
        SecurityType.create(SYS_CTX, accCtx.subjectDomain.name);
        Role.create(SYS_CTX, accCtx.subjectRole.name);

        {
            // TODO each line in this block should be called in the corresponding .create method from the lines above
            insertIdentity(transMgr, accCtx.subjectId.name);
            insertSecType(transMgr, accCtx.subjectDomain.name);
            insertRole(transMgr, accCtx.subjectRole.name, accCtx.subjectDomain.name);
        }

        ObjectProtection nodesMapProt = nodeRepository.getObjProt();
        ObjectProtection rscDfnMapProt = resourceDefinitionRepository.getObjProt();
        ObjectProtection storPoolDfnMapProt = storPoolDefinitionRepository.getObjProt();
        nodesMapProt.getSecurityType().addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CHANGE);
        rscDfnMapProt.getSecurityType().addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CHANGE);
        storPoolDfnMapProt.getSecurityType().addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CHANGE);

        accCtx.subjectDomain.addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CONTROL);

        nodesMapProt.setConnection(transMgr);
        rscDfnMapProt.setConnection(transMgr);
        storPoolDfnMapProt.setConnection(transMgr);
        nodesMapProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);
        rscDfnMapProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);
        storPoolDfnMapProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);

        ObjectProtection disklessStorPoolDfnProt =
            storPoolDefinitionRepository.get(SYS_CTX, new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME)).getObjProt();
        disklessStorPoolDfnProt.setConnection(transMgr);
        disklessStorPoolDfnProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);
    }

    protected Context contextWrite()
    {
        return Context.of(
            ApiModule.API_CALL_NAME, "TestApiCallName",
            AccessContext.class, mockPeer.getAccessContext(),
            Peer.class, mockPeer,
            ApiModule.API_CALL_ID, 1L
        );
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
                Mockito.any(AccessContext.class),
                Mockito.eq(false)
            );
        Mockito.verify(satelliteConnector, Mockito.times(currentCall.expectedAsyncConnectingAttempts.size()))
            .startConnecting(
                Mockito.any(Node.class),
                Mockito.any(AccessContext.class),
                Mockito.eq(true)
            );
    }
}
