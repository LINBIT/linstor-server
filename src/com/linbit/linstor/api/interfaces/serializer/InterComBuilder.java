package com.linbit.linstor.api.interfaces.serializer;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public interface InterComBuilder {
    byte[] build();

    InterComBuilder primaryRequest(String rscName, String rscUuid);

    InterComBuilder authMessage(UUID nodeUuid, String nodeName, byte[] sharedSecret);

    InterComBuilder nodeList(List<Node.NodeApi> nodes);
    InterComBuilder storPoolDfnList(List<StorPoolDefinition.StorPoolDfnApi> storPoolDfns);
    InterComBuilder storPoolList(List<StorPool.StorPoolApi> storOools);
    InterComBuilder resourceDfnList(List<ResourceDefinition.RscDfnApi> rscDfns);
    InterComBuilder resourceList(List<Resource.RscApi> rscs);

    InterComBuilder notifyResourceDeleted(String nodeName, String resourceName, String rscUuid);
    InterComBuilder notifyVolumeDeleted(String nodeName, String resourceName, int volumeNr);

    InterComBuilder requestNodeUpdate(UUID nodeUuid, String nodeName);
    InterComBuilder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName);
    InterComBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName);
    InterComBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName);
}
