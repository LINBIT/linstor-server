package com.linbit.linstor;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.VersionInfoProvider;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Common debug methods of the Controller and Satellite modules
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CommonDebugControl
{
    LinStor getInstance();
    String getProgramName();
    String getModuleType();
    VersionInfoProvider getVersionInfoProvider();
    Map<ServiceName, SystemService> getSystemServiceMap();
    Peer getPeer(String peerId);
    Map<String, Peer> getAllPeers();
    Map<String, ApiCall> getApiCallObjects();
    Map<NodeName, Node> getNodesMap();
    ObjectProtection getNodesMapProt();
    Map<ResourceName, ResourceDefinition> getRscDfnMap();
    ObjectProtection getRscDfnMapProt();
    Map<StorPoolName, StorPoolDefinition> getStorPoolDfnMap();
    ObjectProtection getStorPoolDfnMapProt();
    Props getConf();
    ObjectProtection getConfProt();
    ReadWriteLock getReconfigurationLock();
    ReadWriteLock getConfLock();
    ReadWriteLock getNodesMapLock();
    ReadWriteLock getRscDfnMapLock();
    ReadWriteLock getStorPoolDfnMapLock();
    void shutdown(AccessContext accCtx);
}
