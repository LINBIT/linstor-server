package com.linbit.linstor.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Assert;
import org.junit.Before;
import com.linbit.TransactionMgr;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.utils.NetInterfaceApiTestImpl;
import com.linbit.linstor.api.utils.SatelliteConnectionApiTestImpl;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.api.utils.ApiCtrlAccessorTestImpl;
import com.linbit.linstor.api.utils.DummyTcpConnector;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.numberpool.BitmapPool;
import com.linbit.linstor.numberpool.NumberPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.SecurityType;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.testclient.ClientProtobuf;

public abstract class ApiTestBase extends DerbyBase
{
    protected final static long CRT = ApiConsts.MASK_CRT;
    protected final static long DEL = ApiConsts.MASK_DEL;
    protected final static long MOD = ApiConsts.MASK_MOD;

    protected static final AccessContext ALICE_ACC_CTX;
    protected static final AccessContext BOB_ACC_CTX;

    static
    {
        ALICE_ACC_CTX = TestAccessContextProvider.ALICE_ACC_CTX;
        BOB_ACC_CTX = TestAccessContextProvider.BOB_ACC_CTX;
    }

    /*
     * Controller fields START
     */
    protected static MetaDataApi metaData;
    protected static ObjectProtection nodesMapProt;
    protected static ObjectProtection rscDfnMapProt;
    protected static ObjectProtection storPoolDfnMapProt;
    protected static ReadWriteLock nodesMapLock;
    protected static ReadWriteLock rscDfnMapLock;
    protected static ReadWriteLock storPoolDfnMapLock;
    protected static ReadWriteLock ctrlConfLock;
    protected static Props ctrlConf;
    protected static ObjectProtection ctrlConfProt;
    /*
     * Controller fields END
     */
    private static TcpConnector tcpConnector;
    private static NumberPool minorNrPool = new BitmapPool(MinorNumber.MINOR_NR_MAX + 1);
    private static NumberPool tcpPortNrPool = new BitmapPool(TcpPortNumber.PORT_NR_MAX + 1);


    private static ApiCtrlAccessorTestImpl testApiCtrlAccessors;
    protected static CtrlApiCallHandler apiCallHandler;

    public ApiTestBase()
    {
        tcpConnector = new DummyTcpConnector();
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        metaData = new MetaData();

        TransactionMgr transMgr = new TransactionMgr(dbConnPool);

        nodesMapProt = ObjectProtection.getInstance(SYS_CTX, "/sys/controller/nodesMap", true, transMgr);
        rscDfnMapProt = ObjectProtection.getInstance(SYS_CTX, "/sys/controller/rscDfnMap", true, transMgr);
        storPoolDfnMapProt = ObjectProtection.getInstance(SYS_CTX, "/sys/controller/storPoolMap", true, transMgr);

        nodesMapLock = new ReentrantReadWriteLock(true);
        rscDfnMapLock = new ReentrantReadWriteLock(true);
        storPoolDfnMapLock = new ReentrantReadWriteLock(true);
        ctrlConfLock = new ReentrantReadWriteLock(true);

        ctrlConf = PropsContainer.getInstance("CTRLCFG", transMgr);
        ctrlConfProt = ObjectProtection.getInstance(SYS_CTX, "/sys/controller/conf", true, transMgr);

        ctrlConf.setProp(Controller.PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC, "ignore");
        ctrlConf.setProp(Controller.PROPSCON_KEY_DEFAULT_SSL_CON_SVC, "ignore");

        create(transMgr, ALICE_ACC_CTX);
        create(transMgr, BOB_ACC_CTX);

        transMgr.commit();
        dbConnPool.returnConnection(transMgr);

        testApiCtrlAccessors = new ApiCtrlAccessorTestImpl(
            ctrlConfLock,
            ctrlConf,
            nodesMap,
            nodesMapProt,
            nodesMapLock,
            rscDfnMap,
            rscDfnMapProt,
            rscDfnMapLock,
            storPoolDfnMap,
            storPoolDfnMapProt,
            storPoolDfnMapLock,
            errorReporter,
            dbConnPool,
            tcpConnector,
            metaData
        );

        apiCallHandler = new CtrlApiCallHandler(
            testApiCtrlAccessors,
            ApiType.PROTOBUF,
            minorNrPool,
            tcpPortNrPool,
            SYS_CTX
        );
    }

    private void create(TransactionMgr transMgr, AccessContext accCtx) throws AccessDeniedException, SQLException
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

        nodesMapProt.getSecurityType().addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CHANGE);
        rscDfnMapProt.getSecurityType().addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CHANGE);
        storPoolDfnMapProt.getSecurityType().addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CHANGE);

        accCtx.subjectDomain.addRule(SYS_CTX, accCtx.subjectDomain, AccessType.CONTROL);

        nodesMapProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);
        rscDfnMapProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);
        storPoolDfnMapProt.addAclEntry(SYS_CTX, accCtx.subjectRole, AccessType.CHANGE);
    }

    protected static NetInterfaceApi createNetInterfaceApi(String name, String address)
    {
        return createNetInterfaceApi(java.util.UUID.randomUUID(), name, address);
    }

    protected static NetInterfaceApi createNetInterfaceApi(java.util.UUID uuid, String name, String address)
    {
        return new NetInterfaceApiTestImpl(uuid, name, address);
    }

    protected static SatelliteConnectionApi createStltConnApi(String netIfName)
    {
        return createStltConnApi(netIfName, ApiConsts.DFLT_STLT_PORT_PLAIN, ApiConsts.VAL_NETCOM_TYPE_PLAIN);
    }

    protected static SatelliteConnectionApi createStltConnApi(
        String netIfName,
        Integer port,
        String encryptionType
    )
    {
        return new SatelliteConnectionApiTestImpl(netIfName, port, encryptionType);
    }

    public String generateSharedSecret()
    {
        return testApiCtrlAccessors.generateSharedSecret();
    }

    protected void expectRc(long index, long expectedRc, RcEntry rcEntry)
    {
        if (rcEntry.getReturnCode() != expectedRc)
        {
            Assert.fail("Expected [" + index + "] RC to be " + resolveRC(expectedRc) + " but got " + resolveRC(rcEntry.getReturnCode()));
        }
    }

    private String resolveRC(long expectedRc)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        ClientProtobuf.appendReadableRetCode(sb, expectedRc);
        sb.append("]");
        return sb.toString();
    }

    protected RcEntry checkedGet(ApiCallRc rc, int idx)
    {
        assertThat(rc.getEntries().size()).isGreaterThanOrEqualTo(idx + 1);

        return rc.getEntries().get(idx);
    }

    protected RcEntry checkedGet(ApiCallRc rc, int idx, int expectedSize)
    {
        assertThat(expectedSize).isGreaterThan(idx);
        assertThat(rc.getEntries()).hasSize(expectedSize);

        return rc.getEntries().get(idx);
    }

    protected void evaluateTestSequence(AbsApiCallTester... callSequence)
    {
        for (AbsApiCallTester currentCall : callSequence)
        {
            evaluateTest(currentCall);
        }
    }

    protected void evaluateTest(AbsApiCallTester currentCall)
    {
        testApiCtrlAccessors.stltConnectingAttempts.clear();

        ApiCallRc rc = currentCall.executeApiCall();

        List<Long> expectedRetCodes = currentCall.retCodes;
        List<RcEntry> actualRetCodes = rc.getEntries();

        assertThat(expectedRetCodes).hasSameSizeAs(actualRetCodes);
        for (int idx = 0; idx < expectedRetCodes.size(); idx++)
        {
            expectRc(idx, expectedRetCodes.get(idx), actualRetCodes.get(idx));
        }

        assertThat(currentCall.expectedConnectingAttempts)
            .hasSameSizeAs(testApiCtrlAccessors.stltConnectingAttempts)
        ;
    }
}
