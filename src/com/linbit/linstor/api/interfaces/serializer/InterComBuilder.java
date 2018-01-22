package com.linbit.linstor.api.interfaces.serializer;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.api.pojo.ResourceState;

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
    InterComBuilder resourceList(final List<Resource.RscApi> rscs, final Collection<ResourceState> rscStates);

    InterComBuilder resourceState(final String nodeName, final ResourceState rsc);

    InterComBuilder notifyResourceDeleted(String nodeName, String resourceName, String rscUuid);
    InterComBuilder notifyVolumeDeleted(String nodeName, String resourceName, int volumeNr);

    InterComBuilder requestNodeUpdate(UUID nodeUuid, String nodeName);
    InterComBuilder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName);
    InterComBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName);
    InterComBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName);

    InterComBuilder changedNode(UUID nodeUuid, String nodeName);
    InterComBuilder changedResource(UUID nodeUuid, String nodeName);
    InterComBuilder changedStorPool(UUID nodeUuid, String nodeName);

    InterComBuilder nodeData(Node node, Collection<Node> relatedNodes);
    InterComBuilder resourceData(Resource localResource);
    InterComBuilder storPoolData(StorPool storPool);
    InterComBuilder fullSync(Set<Node> nodeSet, Set<StorPool> storPools, Set<Resource> resources);
}
