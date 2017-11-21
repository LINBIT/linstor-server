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
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.api.ApiCall;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.concurrent.locks.ReadWriteLock;

class CtrlDebugControlImpl implements CtrlDebugControl
{
    private final Controller controller;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private final Map<String, Peer> peerMap;
    private final CommonMessageProcessor msgProc;

    CtrlDebugControlImpl(
        Controller controllerRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        Map<String, Peer> peerMapRef,
        CommonMessageProcessor msgProcRef)
    {
        controller = controllerRef;
        systemServicesMap = systemServicesMapRef;
        peerMap = peerMapRef;
        msgProc = msgProcRef;
    }

    @Override
    public DrbdManage getInstance()
    {
        return controller;
    }

    @Override
    public Controller getModuleInstance()
    {
        return controller;
    }

    @Override
    public String getProgramName()
    {
        return Controller.PROGRAM;
    }

    @Override
    public String getModuleType()
    {
        return Controller.MODULE;
    }

    @Override
    public String getVersion()
    {
        return Controller.VERSION;
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
        return controller.nodesMap;
    }

    @Override
    public Map<ResourceName, ResourceDefinition> getRscDfnMap()
    {
        return controller.rscDfnMap;
    }

    @Override
    public Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap()
    {
        return controller.storPoolDfnMap;
    }

    @Override
    public Props getConf()
    {
        return controller.ctrlConf;
    }

    public DbConnectionPool getDbConnectionPool()
    {
        return controller.dbConnPool;
    }

    @Override
    public void shutdown(AccessContext accCtx)
    {
        try
        {
            controller.shutdown(accCtx);
        }
        catch (AccessDeniedException accExc)
        {
            controller.getErrorReporter().reportError(accExc);
        }
    }

    @Override
    public ReadWriteLock getReconfigurationLock()
    {
        return controller.reconfigurationLock;
    }

    @Override
    public ReadWriteLock getConfLock()
    {
        return controller.ctrlConfLock;
    }

    @Override
    public ReadWriteLock getNodesMapLock()
    {
        return controller.nodesMapLock;
    }

    @Override
    public ReadWriteLock getRscDfnMapLock()
    {
        return controller.rscDfnMapLock;
    }

    @Override
    public ReadWriteLock getStorPoolDfnMapLock()
    {
        return controller.storPoolDfnMapLock;
    }

    @Override
    public ObjectProtection getNodesMapProt()
    {
        return controller.nodesMapProt;
    }

    @Override
    public ObjectProtection getRscDfnMapProt()
    {
        return controller.rscDfnMapProt;
    }

    @Override
    public ObjectProtection getStorPoolDfnMapProt()
    {
        return controller.storPoolDfnMapProt;
    }

    @Override
    public ObjectProtection getConfProt()
    {
        return controller.ctrlConfProt;
    }
}