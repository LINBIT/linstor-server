package com.linbit.linstor.core;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

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
    public ReadWriteLock getCtrlConfigLock();
    public Props getCtrlConf();

    /*
     * Node accessors
     */
    public Map<NodeName, Node> getNodesMap();
    public ObjectProtection getNodesMapProtection();
    public ReadWriteLock getNodesMapLock();

    /*
     * ResourceDefinition accessors
     */
    public Map<ResourceName, ResourceDefinition> getRscDfnMap();
    public ObjectProtection getRscDfnMapProtection();
    public ReadWriteLock getRscDfnMapLock();
    public String generateSharedSecret();

    public void cleanup();
    public short getDefaultPeerCount();
    public int getDefaultAlStripes();

    /*
     * StorPoolDefinition accessors
     */
    public Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap();
    public ObjectProtection getStorPoolDfnMapProtection();
    public ReadWriteLock getStorPoolDfnMapLock();
    public String getDefaultStorPoolName();

    /*
     * Other accessors
     */
    public ErrorReporter getErrorReporter();
    public DbConnectionPool getDbConnPool();

    public TcpConnector getNetComConnector(ServiceName dfltConSvcName);
    public void connectSatellite(InetSocketAddress inetSocketAddress, TcpConnector tcpConnector, Node node);
    public MetaDataApi getMetaDataApi();
}
