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

/**
 * Simple adapter that grants the API classes access
 * to controller's core objects.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
class ApiCtrlAccessorImpl implements ApiCtrlAccessors
{
    private Controller controller;

    ApiCtrlAccessorImpl(Controller controllerRef)
    {
        controller = controllerRef;
    }

    @Override
    public ReadWriteLock getCtrlConfigLock()
    {
        return controller.ctrlConfLock;
    }

    @Override
    public ObjectProtection getCtrlConfProtection()
    {
        return controller.ctrlConfProt;
    }

    @Override
    public Props getCtrlConf()
    {
        return controller.ctrlConf;
    }

    @Override
    public Map<NodeName, Node> getNodesMap()
    {
        return controller.nodesMap;
    }

    @Override
    public ObjectProtection getNodesMapProtection()
    {
        return controller.nodesMapProt;
    }

    @Override
    public ReadWriteLock getNodesMapLock()
    {
        return controller.nodesMapLock;
    }

    @Override
    public Map<ResourceName, ResourceDefinition> getRscDfnMap()
    {
        return controller.rscDfnMap;
    }

    @Override
    public ObjectProtection getRscDfnMapProtection()
    {
        return controller.rscDfnMapProt;
    }

    @Override
    public ReadWriteLock getRscDfnMapLock()
    {
        return controller.rscDfnMapLock;
    }

    @Override
    public Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap()
    {
        return controller.storPoolDfnMap;
    }

    @Override
    public ObjectProtection getStorPoolDfnMapProtection()
    {
        return controller.storPoolDfnMapProt;
    }

    @Override
    public ReadWriteLock getStorPoolDfnMapLock()
    {
        return controller.storPoolDfnMapLock;
    }

    @Override
    public ErrorReporter getErrorReporter()
    {
        return controller.getErrorReporter();
    }

    @Override
    public DbConnectionPool getDbConnPool()
    {
        return controller.dbConnPool;
    }

    @Override
    public String generateSharedSecret()
    {
        return controller.generateSharedSecret();
    }

    @Override
    public void connectSatellite(InetSocketAddress inetSocketAddress, TcpConnector tcpConnector, Node node)
    {
        controller.connectSatellite(inetSocketAddress, tcpConnector, node);
    }

    @Override
    public TcpConnector getNetComConnector(ServiceName conSvcName)
    {
        return controller.netComConnectors.get(conSvcName);
    }

    @Override
    public String getDefaultStorPoolName()
    {
        return controller.getDefaultStorPoolName();
    }

    @Override
    public MetaDataApi getMetaDataApi()
    {
        return controller.getMetaDataApi();
    }

    @Override
    public int getDefaultAlStripes()
    {
        return controller.getDefaultAlStripes();
    }

    @Override
    public short getDefaultPeerCount()
    {
        return controller.getDefaultPeerCount();
    }

    @Override
    public int getFreeTcpPort() throws ExhaustedPoolException
    {
        return controller.getFreeTcpPort();
    }

    @Override
    public int getFreeMinorNr() throws ExhaustedPoolException
    {
        return controller.getFreeMinorNr();
    }

    @Override
    public void reloadTcpPortRange()
    {
        controller.reloadTcpPortRange();
    }

    @Override
    public void reloadMinorNrRange()
    {
        controller.reloadMinorNrRange();
    }
}
