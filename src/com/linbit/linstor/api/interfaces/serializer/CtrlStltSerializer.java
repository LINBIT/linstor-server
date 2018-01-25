package com.linbit.linstor.api.interfaces.serializer;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.pojo.ResourceState;

public interface CtrlStltSerializer
{
    public Builder builder(String apiCall, int msgId);

    public interface Builder
    {
        byte[] build();

        /*
         * Controller -> Satellite
         */
        Builder primaryRequest(String rscName, String rscUuid);

        Builder authMessage(
            UUID nodeUuid,
            String nodeName,
            byte[] sharedSecret,
            UUID nodeDisklessStorPoolDfnUuid,
            UUID nodeDisklessStorPoolUuid
        );

        Builder changedNode(UUID nodeUuid, String nodeName);
        Builder changedResource(UUID rscUuid, String rscName);
        Builder changedStorPool(UUID storPoolUuid, String storPoolName);

        Builder nodeData(Node node, Collection<Node> relatedNodes);
        Builder resourceData(Resource localResource);
        Builder storPoolData(StorPool storPool);
        Builder fullSync(
            Set<Node> nodeSet,
            Set<StorPool> storPools,
            Set<Resource> resources
        );

        /*
         * Satellite -> Controller
         */
        Builder resourceState(final String nodeName, final ResourceState rsc);

        Builder notifyResourceDeleted(String nodeName, String resourceName, UUID rscUuid);
        Builder notifyVolumeDeleted(String nodeName, String resourceName, int volumeNr, UUID vlmUuid);

        Builder requestNodeUpdate(UUID nodeUuid, String nodeName);
        Builder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName);
        Builder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName);
        Builder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName);

    }
}
