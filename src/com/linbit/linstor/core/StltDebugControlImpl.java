package com.linbit.linstor.core;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;

class StltDebugControlImpl implements StltDebugControl
{
    private final Satellite satellite;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private final Map<String, Peer> peerMap;
    private final CommonMessageProcessor msgProc;

    StltDebugControlImpl(
        Satellite satelliteRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        Map<String, Peer> peerMapRef,
        CommonMessageProcessor msgProcRef
    )
    {
        satellite = satelliteRef;
        systemServicesMap = systemServicesMapRef;
        peerMap = peerMapRef;
        msgProc = msgProcRef;
    }

    @Override
    public LinStor getInstance()
    {
        return satellite;
    }

    @Override
    public Satellite getModuleInstance()
    {
        return satellite;
    }

    @Override
    public String getProgramName()
    {
        return Satellite.PROGRAM;
    }

    @Override
    public String getModuleType()
    {
        return Satellite.MODULE;
    }

    @Override
    public String getVersion()
    {
        return Satellite.VERSION;
    }

    @Override
    public Map<ServiceName, SystemService> getSystemServiceMap()
    {
        Map<ServiceName, SystemService> svcCpy = new TreeMap<>();
        svcCpy.putAll(systemServicesMap);
        return svcCpy;
    }

    @Override
    public Peer getPeer(String peerId)
    {
        Peer peerObj = null;
        synchronized (peerMap)
        {
            peerObj = peerMap.get(peerId);
        }
        return peerObj;
    }

    @Override
    public Map<String, Peer> getAllPeers()
    {
        TreeMap<String, Peer> peerMapCpy = new TreeMap<>();
        synchronized (peerMap)
        {
            peerMapCpy.putAll(peerMap);
        }
        return peerMapCpy;
    }

    @Override
    public Set<String> getApiCallNames()
    {
        return msgProc.getApiCallNames();
    }

    @Override
    public Map<String, ApiCall> getApiCallObjects()
    {
        return msgProc.getApiCallObjects();
    }

    @Override
    public Map<NodeName, Node> getNodesMap()
    {
        return satellite.nodesMap;
    }

    @Override
    public Map<ResourceName, ResourceDefinition> getRscDfnMap()
    {
        return satellite.rscDfnMap;
    }

    @Override
    public Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap()
    {
        return satellite.storPoolDfnMap;
    }

    @Override
    public Props getConf()
    {
        return satellite.stltConf;
    }

    @Override
    public void shutdown(AccessContext accCtx)
    {
        try
        {
            satellite.shutdown(accCtx);
        }
        catch (AccessDeniedException accExc)
        {
            satellite.getErrorReporter().reportError(accExc);
        }
    }

    @Override
    public ReadWriteLock getReconfigurationLock()
    {
        return satellite.reconfigurationLock;
    }

    @Override
    public ReadWriteLock getConfLock()
    {
        return satellite.stltConfLock;
    }

    @Override
    public ReadWriteLock getNodesMapLock()
    {
        return satellite.nodesMapLock;
    }

    @Override
    public ReadWriteLock getRscDfnMapLock()
    {
        return satellite.rscDfnMapLock;
    }

    @Override
    public ReadWriteLock getStorPoolDfnMapLock()
    {
        return satellite.storPoolDfnMapLock;
    }

    @Override
    public ObjectProtection getNodesMapProt()
    {
        return null;
    }

    @Override
    public ObjectProtection getRscDfnMapProt()
    {
        return null;
    }

    @Override
    public ObjectProtection getStorPoolDfnMapProt()
    {
        return null;
    }

    @Override
    public ObjectProtection getConfProt()
    {
        return null;
    }
}