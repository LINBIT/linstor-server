package com.linbit.linstor.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Assert;
import org.junit.Before;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.TransactionMgr;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.testclient.ClientProtobuf;
import com.linbit.utils.Base64;

public class ApiTestBase extends DerbyBase
{
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
    protected MetaDataApi metaData;
    protected ObjectProtection nodesMapProt;
    protected ObjectProtection rscDfnMapProt;
    protected ObjectProtection storPoolDfnMapProt;
    protected ReadWriteLock nodesMapLock;
    protected ReadWriteLock rscDfnMapLock;
    protected ReadWriteLock storPoolDfnMapLock;
    protected ReadWriteLock ctrlConfLock;
    protected Props ctrlConf;
    protected ObjectProtection ctrlConfProt;
    /*
     * Controller fields END
     */
    private TcpConnector tcpConnector;


    private ApiCtrlAccessors testApiCtrlAccessors;
    protected CtrlApiCallHandler apiCallHandler;

    public ApiTestBase()
    {
        testApiCtrlAccessors = new TestApiCtrlAccessorImpl();

        tcpConnector = new TestTcpConnector();
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

        transMgr.commit();

        apiCallHandler = new CtrlApiCallHandler(testApiCtrlAccessors, ApiType.PROTOBUF, SYS_CTX);
    }

    protected NetInterfaceApi createNetInterfaceApi(String name, String address)
    {
        return createNetInterfaceApi(java.util.UUID.randomUUID(), name, address);
    }

    protected NetInterfaceApi createNetInterfaceApi(final java.util.UUID uuid, final String name, final String address)
    {
        return new NetInterfaceApi()
        {
            @Override
            public java.util.UUID getUuid()
            {
                return uuid;
            }

            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public String getAddress()
            {
                return address;
            }
        };
    }

    protected SatelliteConnectionApi createStltConnApi(String netIfName)
    {
        return createStltConnApi(netIfName, ApiConsts.DFLT_STLT_PORT_PLAIN, ApiConsts.VAL_NETCOM_TYPE_PLAIN);
    }

    protected SatelliteConnectionApi createStltConnApi(
        final String netIfName,
        final Integer port,
        final String encryptionType
    )
    {
        return new SatelliteConnectionApi()
        {

            @Override
            public String getNetInterfaceName()
            {
                return netIfName;
            }

            @Override
            public int getPort()
            {
                return port;
            }

            @Override
            public String getEncryptionType()
            {
                return encryptionType;
            }
        };
    }

    public String generateSharedSecret()
    {
        byte[] randomBytes = new byte[15];
        new SecureRandom().nextBytes(randomBytes);
        String secret = Base64.encode(randomBytes);
        return secret;
    }

    protected void expectRc(RcEntry rcEntry, long expectedRc)
    {
        if (rcEntry.getReturnCode() != expectedRc)
        {
            Assert.fail("Expected RC " + resolveRC(expectedRc) + " but got " + resolveRC(rcEntry.getReturnCode()));
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

    private class TestApiCtrlAccessorImpl implements ApiCtrlAccessors
    {
        @Override
        public ReadWriteLock getCtrlConfigLock()
        {
            return ctrlConfLock;
        }

        @Override
        public Props getCtrlConf()
        {
            return ctrlConf;
        }

        @Override
        public Map<NodeName, Node> getNodesMap()
        {
            return nodesMap;
        }

        @Override
        public ObjectProtection getNodesMapProtection()
        {
            return nodesMapProt;
        }

        @Override
        public ReadWriteLock getNodesMapLock()
        {
            return nodesMapLock;
        }

        @Override
        public Map<ResourceName, ResourceDefinition> getRscDfnMap()
        {
            return rscDfnMap;
        }

        @Override
        public ObjectProtection getRscDfnMapProtection()
        {
            return rscDfnMapProt;
        }

        @Override
        public ReadWriteLock getRscDfnMapLock()
        {
            return rscDfnMapLock;
        }

        @Override
        public String generateSharedSecret()
        {
            return ApiTestBase.this.generateSharedSecret();
        }

        @Override
        public void cleanup()
        {
            // ignore for now
        }

        @Override
        public short getDefaultPeerCount()
        {
            return Controller.DEFAULT_PEER_COUNT;
        }

        @Override
        public int getDefaultAlStripes()
        {
            return Controller.DEFAULT_AL_STRIPES;
        }

        @Override
        public Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap()
        {
            return storPoolDfnMap;
        }

        @Override
        public ObjectProtection getStorPoolDfnMapProtection()
        {
            return storPoolDfnMapProt;
        }

        @Override
        public ReadWriteLock getStorPoolDfnMapLock()
        {
            return storPoolDfnMapLock;
        }

        @Override
        public String getDefaultStorPoolName()
        {
            return Controller.DEFAULT_STOR_POOL_NAME;
        }

        @Override
        public ErrorReporter getErrorReporter()
        {
            return errorReporter;
        }

        @Override
        public DbConnectionPool getDbConnPool()
        {
            return dbConnPool;
        }

        @Override
        public TcpConnector getNetComConnector(ServiceName dfltConSvcName)
        {
            return tcpConnector;
        }

        @Override
        public void connectSatellite(InetSocketAddress inetSocketAddress, TcpConnector tcpConnector, Node node)
        {
            // sure!   ....  ignore
        }

        @Override
        public MetaDataApi getMetaDataApi()
        {
            return metaData;
        }
    }

    private class TestTcpConnector implements TcpConnector
    {
        @Override
        public void setServiceInstanceName(ServiceName instanceName)
        {
            // no-op
        }

        @Override
        public void start() throws SystemServiceStartException
        {
            // no-op
        }

        @Override
        public void shutdown()
        {
            // no-op
        }

        @Override
        public void awaitShutdown(long timeout) throws InterruptedException
        {
            // no-op
        }

        @Override
        public ServiceName getServiceName()
        {
            return null;
        }

        @Override
        public String getServiceInfo()
        {
            return null;
        }

        @Override
        public ServiceName getInstanceName()
        {
            return null;
        }

        @Override
        public boolean isStarted()
        {
            return false;
        }

        @Override
        public Peer connect(InetSocketAddress address, Node node) throws IOException
        {
            // no-op
            return null;
        }

        @Override
        public Peer reconnect(Peer peer) throws IOException
        {
            // no-op
            return null;
        }

        @Override
        public void closeConnection(TcpConnectorPeer peerObj)
        {
            // no-op
        }

        @Override
        public void wakeup()
        {
            // no-op
        }
    }
}
