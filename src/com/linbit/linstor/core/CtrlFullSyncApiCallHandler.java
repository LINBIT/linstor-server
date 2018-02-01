package com.linbit.linstor.core;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

class CtrlFullSyncApiCallHandler
{
    private ApiCtrlAccessors apiCtrlAccessors;
    private AccessContext apiCtx;
    private CtrlStltSerializer interComSerializer;

    CtrlFullSyncApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        AccessContext apiCtxRef,
        CtrlStltSerializer interComSerializerRef
    )
    {
        apiCtrlAccessors = apiCtrlAccessorsRef;
        apiCtx = apiCtxRef;
        interComSerializer = interComSerializerRef;
    }

    void sendFullSync(Peer satellite, long expectedFullSyncId)
    {
        try
        {
            Node localNode = satellite.getNode();

            Set<Node> nodes = new LinkedHashSet<>();
            Set<StorPool> storPools = new LinkedHashSet<>();
            Set<Resource> rscs = new LinkedHashSet<>();

            nodes.add(localNode); // always add the localNode

            Iterator<Resource> rscIterator = localNode.iterateResources(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();
                rscs.add(rsc);
                Iterator<Resource> otherRscIterator = rsc.getDefinition().iterateResource(apiCtx);
                while (otherRscIterator.hasNext())
                {
                    Resource otherRsc = otherRscIterator.next();
                    if (otherRsc != rsc)
                    {
                        nodes.add(otherRsc.getAssignedNode());
                    }
                }
            }
            // some storPools might have been created on the satellite, but are not used by resources / volumes
            // however, when a rsc / vlm is created, they already assume the referenced storPool already exists
            Iterator<StorPool> storPoolIterator = localNode.iterateStorPools(apiCtx);
            while (storPoolIterator.hasNext())
            {
                storPools.add(storPoolIterator.next());
            }

            satellite.setFullSyncId(expectedFullSyncId);

            byte[] data = interComSerializer
                .builder(InternalApiConsts.API_FULL_SYNC_DATA, 0)
                .fullSync(nodes, storPools, rscs, expectedFullSyncId, -1) // fullSync has -1 as updateId
                .build();
            apiCtrlAccessors.getErrorReporter().logTrace("Sending full sync to satellite '" + satellite.getId() + "'.");
            Message msg = satellite.createMessage();
            msg.setData(data);
            satellite.sendMessage(msg);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "ApiCtx does not have enough privileges to create a full sync for satellite " + satellite.getId(),
                    accDeniedExc
                )
            );

        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to send a full sync to the satellite " + satellite.getId(),
                    illegalMessageStateExc
                )
            );
        }

    }
}
