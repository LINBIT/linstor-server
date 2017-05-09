package com.linbit.drbdmanage;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import java.util.Map;

/**
 * Common debug methods of the Controller and Satellite modules
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CommonDebugControl
{
    String getProgramName();
    String getModuleType();
    String getVersion();
    Map<ServiceName, SystemService> getSystemServiceMap();
    Peer getPeer(String peerId);
    Map<String, Peer> getAllPeers();
    void shutdown(AccessContext accCtx);
}
