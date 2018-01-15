package com.linbit.linstor.api.interfaces.serializer;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
public interface InterComBuilder {
    public byte[] build();

    public InterComBuilder primaryRequest(String rscName, String rscUuid);

    public InterComBuilder nodeList(List<Node.NodeApi> nodes);
    public InterComBuilder storPoolDfnList(List<StorPoolDefinition.StorPoolDfnApi> storPoolDfns);
    public InterComBuilder storPoolList(List<StorPool.StorPoolApi> storOools);
    public InterComBuilder resourceDfnList(List<ResourceDefinition.RscDfnApi> rscDfns);
    public InterComBuilder resourceList(List<Resource.RscApi> rscs);

    InterComBuilder notifyResourceDeleted(String nodeName, String resourceName, String rscUuid);
    InterComBuilder notifyVolumeDeleted(String nodeName, String resourceName, int volumeNr);
}
