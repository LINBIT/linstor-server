package com.linbit.linstor.api.utils;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import com.linbit.ExhaustedPoolException;
import com.linbit.ServiceName;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.core.ApiCtrlAccessors;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.Base64;

public class ApiCtrlAccessorTestImpl implements ApiCtrlAccessors
{
    public static class StltConnectingAttempt
    {
        public InetSocketAddress inetSocketAddress;
        public TcpConnector tcpConnector;
        public Node node;

        public StltConnectingAttempt(InetSocketAddress inetSocketAddress, TcpConnector tcpConnector, Node node)
        {
            this.inetSocketAddress = inetSocketAddress;
            this.tcpConnector = tcpConnector;
            this.node = node;
        }
    }

    private ReadWriteLock ctrlConfLock;
    private Props ctrlConf;
    private Map<NodeName, Node> nodesMap;
    private ObjectProtection nodesMapProt;
    private ReadWriteLock nodesMapLock;
    private Map<ResourceName, ResourceDefinition> rscDfnMap;
    private ObjectProtection rscDfnMapProt;
    private ReadWriteLock rscDfnMapLock;
    private Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;
    private ObjectProtection storPoolDfnMapProt;
    private ReadWriteLock storPoolDfnMapLock;
    private ErrorReporter errorReporter;
    private DbConnectionPool dbConnPool;
    private TcpConnector tcpConnector;
    private MetaDataApi metaData;

    private AtomicInteger tcpPortGen = new AtomicInteger(7000);
    private AtomicInteger minorNrGen = new AtomicInteger(10000);

    public List<StltConnectingAttempt> stltConnectingAttempts;
    private ObjectProtection ctrlCfgProt;

    public ApiCtrlAccessorTestImpl(
        ObjectProtection ctrlCfgProt,
        ReadWriteLock ctrlConfLock,
        Props ctrlConf,
        Map<NodeName, Node> nodesMap,
        ObjectProtection nodesMapProt,
        ReadWriteLock nodesMapLock,
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        ObjectProtection rscDfnMapProt,
        ReadWriteLock rscDfnMapLock,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMap,
        ObjectProtection storPoolDfnMapProt,
        ReadWriteLock storPoolDfnMapLock,
        ErrorReporter errorReporter,
        DbConnectionPool dbConnPool,
        TcpConnector tcpConnector,
        MetaDataApi metaData
    )
    {
        super();
        this.ctrlCfgProt = ctrlCfgProt;
        this.ctrlConfLock = ctrlConfLock;
        this.ctrlConf = ctrlConf;
        this.nodesMap = nodesMap;
        this.nodesMapProt = nodesMapProt;
        this.nodesMapLock = nodesMapLock;
        this.rscDfnMap = rscDfnMap;
        this.rscDfnMapProt = rscDfnMapProt;
        this.rscDfnMapLock = rscDfnMapLock;
        this.storPoolDfnMap = storPoolDfnMap;
        this.storPoolDfnMapProt = storPoolDfnMapProt;
        this.storPoolDfnMapLock = storPoolDfnMapLock;
        this.errorReporter = errorReporter;
        this.dbConnPool = dbConnPool;
        this.tcpConnector = tcpConnector;
        this.metaData = metaData;

        this.stltConnectingAttempts = new ArrayList<>();
    }

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
    public short getDefaultPeerCount()
    {
        return ConfigModule.DEFAULT_PEER_COUNT;
    }

    @Override
    public int getDefaultAlStripes()
    {
        return ConfigModule.DEFAULT_AL_STRIPES;
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
        return ConfigModule.DEFAULT_STOR_POOL_NAME;
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
        stltConnectingAttempts.add(
            new StltConnectingAttempt(
                inetSocketAddress,
                tcpConnector,
                node
            )
        );
    }

    @Override
    public MetaDataApi getMetaDataApi()
    {
        return metaData;
    }

    @Override
    public int getFreeTcpPort() throws ExhaustedPoolException
    {
        return tcpPortGen.getAndIncrement();
    }

    @Override
    public int getFreeMinorNr() throws ExhaustedPoolException
    {
        return minorNrGen.getAndIncrement();
    }

    @Override
    public ObjectProtection getCtrlConfProtection()
    {
        return ctrlCfgProt;
    }

    @Override
    public void reloadTcpPortRange()
    {
        // ignore
    }

    @Override
    public void reloadMinorNrRange()
    {
        // ignore
    }
}