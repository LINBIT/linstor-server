package com.linbit.linstor.testclient;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
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
            accCtx
        );
    }

    public void sendPrimaryRequest(
        final String rscName,
        final String rscUuid
    )
        throws IOException
    {
        send(
            serializer.builder(
                InternalApiConsts.API_PRIMARY_RSC,
                getNextMsgId()
            )
            .primaryRequest(rscName, rscUuid)
            .build()
        );
    }

    public void sendAuthMessage(
        final UUID nodeUuid,
        final String nodeName,
        final byte[] sharedSecret,
        final UUID nodeDisklessStorPoolDfnUuid,
        final UUID nodeDisklessStorPoolUuid
    )
        throws IOException
    {
        send(
            serializer.builder(
                InternalApiConsts.API_AUTH,
                getNextMsgId()
            )
            .authMessage(
                nodeUuid,
                nodeName,
                sharedSecret,
                nodeDisklessStorPoolDfnUuid,
                nodeDisklessStorPoolUuid
            )
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
            serializer.builder(
                InternalApiConsts.API_CHANGED_NODE,
                getNextMsgId()
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
            serializer.builder(
                InternalApiConsts.API_CHANGED_RSC,
                getNextMsgId()
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
            serializer.builder(
                InternalApiConsts.API_CHANGED_STOR_POOL,
                getNextMsgId()
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
            serializer.builder(
                InternalApiConsts.API_APPLY_NODE,
                getNextMsgId()
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
        final String nodeNameStr
    )
        throws IOException
    {
        send(
            serializer.builder(
                InternalApiConsts.API_APPLY_NODE_DELETED,
                getNextMsgId()
            )
            .deletedNodeData(nodeNameStr)
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
            serializer.builder(
                InternalApiConsts.API_APPLY_RSC,
                getNextMsgId()
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
        final String rscNameStr
    )
        throws IOException
    {
        send(
            serializer.builder(
                InternalApiConsts.API_APPLY_RSC_DELETED,
                getNextMsgId()
            )
            .deletedResourceData(rscNameStr)
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
            serializer.builder(
                InternalApiConsts.API_APPLY_STOR_POOL,
                getNextMsgId()
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
        final String nodeNameStr
    )
        throws IOException
    {
        send(
            serializer.builder(
                InternalApiConsts.API_APPLY_STOR_POOL_DELETED,
                getNextMsgId()
            )
            .deletedStorPoolData(nodeNameStr)
            .build()
        );
    }

    public void sendFullSync(
        final Set<Node> nodeSet,
        final Set<StorPool> storPools,
        final Set<Resource> resources,
        final long timestamp,
        final long updateId
    )
        throws IOException
    {
        send(
            serializer.builder(
                InternalApiConsts.API_APPLY_STOR_POOL_DELETED,
                getNextMsgId()
            )
            .fullSync(
                nodeSet,
                storPools,
                resources,
                timestamp,
                updateId
            )
            .build()
        );
    }

}
