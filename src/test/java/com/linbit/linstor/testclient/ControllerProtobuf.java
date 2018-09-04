package com.linbit.linstor.testclient;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

public class ControllerProtobuf extends ProtobufIO
{
    private ProtoCtrlStltSerializer serializer;

    public ControllerProtobuf(
        final String host,
        final int port,
        final AccessContext accCtx
    )
        throws UnknownHostException, IOException
    {
        this(host, port, System.out, accCtx);
    }

    public ControllerProtobuf(
        final String host,
        final int port,
        final PrintStream out,
        final AccessContext accCtx
    )
        throws UnknownHostException, IOException
    {
        super(host, port, out, "ControllerProtobuf");
        serializer = new ProtoCtrlStltSerializer(
            new StderrErrorReporter("ControllerProtobuf"),
            accCtx,
            new CtrlSecurityObjects(),
            null
        );
    }

    public void sendPrimaryRequest(
        final String rscName,
        final String rscUuid
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_PRIMARY_RSC,
                getNextApiCallId()
            )
            .primaryRequest(rscName, rscUuid)
            .build()
        );
    }

    public void sendAuthMessage(final UUID nodeUuid, final String nodeName, final byte[] sharedSecret)
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_AUTH,
                getNextApiCallId()
            )
            .authMessage(nodeUuid, nodeName, sharedSecret)
            .build()
        );
    }

    public void sendChangedNode(
        final UUID nodeUuid,
        final String nodeName
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_CHANGED_NODE,
                getNextApiCallId()
            )
            .changedNode(nodeUuid, nodeName)
            .build()
        );
    }

    public void sendChangedResource(
        final UUID rscUuid,
        final String rscName
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_CHANGED_RSC,
                getNextApiCallId()
            )
            .changedResource(rscUuid, rscName)
            .build()
        );
    }

    public void sendChangedStorPool(
        final UUID storPoolUuid,
        final String storPoolName
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_CHANGED_STOR_POOL,
                getNextApiCallId()
            )
            .changedStorPool(storPoolUuid, storPoolName)
            .build()
        );
    }

    public void sendNodeData(
        final Node node,
        final Collection<Node> relatedNodes,
        final long fullSyncTimestamp,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_NODE,
                getNextApiCallId()
            )
            .nodeData(
                node,
                relatedNodes,
                fullSyncTimestamp,
                updateId
            )
            .build()
        );
    }

    public void sendDeletedNodeData(
        final String nodeNameStr,
        final long fullSyncId,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_NODE_DELETED,
                getNextApiCallId()
            )
            .deletedNodeData(nodeNameStr, fullSyncId, updateId)
            .build()
        );
    }

    public void sendResourceData(
        final Resource localResource,
        final long fullSyncTimestamp,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_RSC,
                getNextApiCallId()
            )
            .resourceData(
                localResource,
                fullSyncTimestamp,
                updateId
            )
            .build()
        );
    }

    public void sendDeletedResourceData(
        final String rscNameStr,
        long fullSyncTimestamp,
        long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_RSC_DELETED,
                getNextApiCallId()
            )
            .deletedResourceData(rscNameStr, fullSyncTimestamp, updateId)
            .build()
        );
    }

    public void sendStorPoolData(
        final StorPool storPool,
        final long fullSyncTimestamp,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_STOR_POOL,
                getNextApiCallId()
            )
            .storPoolData(
                storPool,
                fullSyncTimestamp,
                updateId
            )
            .build()
        );
    }

    public void sendDeletedStorPoolData(
        final String nodeNameStr,
        final long fullSyncId,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_STOR_POOL_DELETED,
                getNextApiCallId()
            )
            .deletedStorPoolData(nodeNameStr, fullSyncId, updateId)
            .build()
        );
    }

    public void sendFullSync(
        final Set<Node> nodeSet,
        final Set<StorPool> storPools,
        final Set<Resource> resources,
        final Set<Snapshot> snapshots,
        final long timestamp,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_APPLY_STOR_POOL_DELETED,
                getNextApiCallId()
            )
            .fullSync(
                nodeSet,
                storPools,
                resources,
                snapshots,
                timestamp,
                updateId
            )
            .build()
        );
    }

}
