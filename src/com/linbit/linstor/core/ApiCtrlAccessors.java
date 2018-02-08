package com.linbit.linstor.core;

import java.net.InetSocketAddress;
import java.util.Map;
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
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.ObjectProtection;

public interface ApiCtrlAccessors
{
    /*
     * Global config accessor
     */
    ReadWriteLock getCtrlConfigLock();
    ObjectProtection getCtrlConfProtection();
    Props getCtrlConf();

    /*
     * Node accessors
     */
    Map<NodeName, Node> getNodesMap();
    ObjectProtection getNodesMapProtection();
    ReadWriteLock getNodesMapLock();

    /*
     * ResourceDefinition accessors
     */
    Map<ResourceName, ResourceDefinition> getRscDfnMap();
    ObjectProtection getRscDfnMapProtection();
    ReadWriteLock getRscDfnMapLock();

    short getDefaultPeerCount();
    int getDefaultAlStripes();

    /*
     * StorPoolDefinition accessors
     */
    Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap();
    ObjectProtection getStorPoolDfnMapProtection();
    ReadWriteLock getStorPoolDfnMapLock();
    String getDefaultStorPoolName();

    /*
     * Other accessors
     */
    ErrorReporter getErrorReporter();
    DbConnectionPool getDbConnPool();

    TcpConnector getNetComConnector(ServiceName dfltConSvcName);
    void connectSatellite(InetSocketAddress inetSocketAddress, TcpConnector tcpConnector, Node node);
    MetaDataApi getMetaDataApi();

    int getFreeTcpPort() throws ExhaustedPoolException;
    int getFreeMinorNr() throws ExhaustedPoolException;

    void reloadTcpPortRange();
    void reloadMinorNrRange();
}
