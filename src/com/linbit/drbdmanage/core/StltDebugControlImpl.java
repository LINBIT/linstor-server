package com.linbit.drbdmanage.core;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

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
    public DrbdManage getInstance()
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
}