package com.linbit.linstor.testclient;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
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
    private final UUID ctrlUuid = UUID.randomUUID();

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
            ReadOnlyPropsImpl.emptyRoProps()
        );
    }

    public void sendPrimaryRequest(
        final String rscName,
        final String rscUuid,
        final boolean alreadyInitialized
    )
        throws IOException
    {
        send(
            serializer.apiCallBuilder(
                InternalApiConsts.API_PRIMARY_RSC,
                getNextApiCallId()
            )
            .primaryRequest(rscName, rscUuid, alreadyInitialized)
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
                .authMessage(nodeUuid, nodeName, sharedSecret, ctrlUuid)
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

    public void sendNode(
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
            .node(
                node,
                relatedNodes,
                fullSyncTimestamp,
                updateId
            )
            .build()
        );
    }

    public void sendDeletedNode(
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
            .deletedNode(nodeNameStr, fullSyncId, updateId)
            .build()
        );
    }

    public void sendResource(
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
            .resource(
                localResource,
                fullSyncTimestamp,
                updateId
            )
            .build()
        );
    }

    public void sendDeletedResource(
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
            .deletedResource(rscNameStr, fullSyncTimestamp, updateId)
            .build()
        );
    }

    public void sendStorPool(
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
            .storPool(
                storPool,
                fullSyncTimestamp,
                updateId
            )
            .build()
        );
    }

    public void sendDeletedStorPool(
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
            .deletedStorPool(nodeNameStr, fullSyncId, updateId)
            .build()
        );
    }

    public void sendFullSync(
        final Set<Node> nodeSet,
        final Set<StorPool> storPools,
        final Set<Resource> resources,
        final Set<Snapshot> snapshots,
        final Set<ExternalFile> externalFiles,
        final Set<AbsRemote> remotes,
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
                externalFiles,
                remotes,
                timestamp,
                updateId
            )
            .build()
        );
    }

}
