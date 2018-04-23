package com.linbit.linstor.event.writer.protobuf.controller;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

@ProtobufEventWriter(
    eventName = ApiConsts.EVENT_RESOURCE_STATE,
    objectType = WatchableObject.RESOURCE
)
@Singleton
public class ResourceStateEvent implements EventWriter
{
    private final CommonSerializer commonSerializer;
    private final CoreModule.NodesMap nodesMap;
    private final ReadWriteLock nodesMapLock;
    private final AccessContext accCtx;

    @Inject
    public ResourceStateEvent(
        CommonSerializer commonSerializerRef,
        CoreModule.NodesMap nodesMapRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @ApiContext AccessContext accCtxRef
    )
    {
        commonSerializer = commonSerializerRef;
        nodesMap = nodesMapRef;
        nodesMapLock = nodesMapLockRef;
        accCtx = accCtxRef;
    }

    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        byte[] eventData;

        nodesMapLock.readLock().lock();
        try
        {
            Node node = nodesMap.get(objectIdentifier.getNodeName());

            String resourceStateString = "NoConnection";
            int upToDateVolumes = 0;
            if (node != null)
            {
                Peer peer = node.getPeer(accCtx);

                if (peer != null)
                {
                    Map<ResourceName, ResourceState> resourceStates = peer.getResourceStates();

                    if (resourceStates != null)
                    {
                        ResourceState resourceState = resourceStates.get(objectIdentifier.getResourceName());

                        if (resourceState != null)
                        {
                            for (VolumeState volumeState : resourceState.getVolumes())
                            {
                                if (DrbdVolume.DS_LABEL_UP_TO_DATE.equals(volumeState.getDiskState()))
                                {
                                    upToDateVolumes++;
                                }
                            }
                        }
                    }
                }

                Resource resource = node.getResource(accCtx, objectIdentifier.getResourceName());

                resourceStateString = upToDateVolumes == resource.getDefinition().getVolumeDfnCount(accCtx) ?
                    "Ready" : "NotReady";
            }

            eventData = commonSerializer.builder().resourceStateEvent(resourceStateString).build();
        }
        finally
        {
            nodesMapLock.readLock().unlock();
        }

        return eventData;
    }
}
