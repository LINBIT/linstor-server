package com.linbit.linstor.api;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer.CtrlStltSerializerBuilder;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class CtrlStltSerializerBuilderImpl extends CommonSerializerBuilderImpl implements CtrlStltSerializerBuilder
{
    private CtrlStltSerializationWriter ctrlStltSerializationWriter;

    public CtrlStltSerializerBuilderImpl(
        ErrorReporter errorReporterRef,
        CtrlStltSerializationWriter ctrlStltSerializationWriterRef,
        String apiCall,
        Integer msgId
    )
    {
        super(errorReporterRef, ctrlStltSerializationWriterRef, apiCall, msgId);

        ctrlStltSerializationWriter = ctrlStltSerializationWriterRef;
    }

    /*
     * Controller -> Satellite
     */
    @Override
    public CtrlStltSerializerBuilder authMessage(
        UUID nodeUuid,
        String nodeName,
        byte[] sharedSecret,
        UUID nodeDisklessStorPoolDfnUuid,
        UUID nodeDisklessStorPoolUuid
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeAuthMessage(
                nodeUuid,
                nodeName,
                sharedSecret,
                nodeDisklessStorPoolDfnUuid,
                nodeDisklessStorPoolUuid,
                baos
            );
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder changedController(UUID nodeUuid, String nodeName)
    {
        try
        {
            ctrlStltSerializationWriter.writeChangedNode(nodeUuid, nodeName, baos); // just appends an object id
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder changedNode(UUID nodeUuid, String nodeName)
    {
        try
        {
            ctrlStltSerializationWriter.writeChangedNode(nodeUuid, nodeName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder changedResource(UUID rscUuid, String rscName)
    {
        try
        {
            ctrlStltSerializationWriter.writeChangedResource(rscUuid, rscName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder changedStorPool(UUID storPoolUuid, String storPoolName)
    {
        try
        {
            ctrlStltSerializationWriter.writeChangedStorPool(storPoolUuid, storPoolName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder controllerData(
        long fullSyncTimestamp,
        long serializerId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeControllerData(fullSyncTimestamp, serializerId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder nodeData(
        Node node,
        Collection<Node> relatedNodes,
        long fullSyncTimestamp,
        long serializerId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeNodeData(node, relatedNodes, fullSyncTimestamp, serializerId, baos);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ProtoInterComSerializer has not enough privileges to seriailze node",
                    accDeniedExc
                )
            );
            exceptionOccured = true;
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder deletedNodeData(
        String nodeNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeDeletedNodeData(nodeNameStr, fullSyncTimestamp, updateId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder resourceData(
        Resource localResource,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeResourceData(localResource, fullSyncTimestamp, updateId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ProtoInterComSerializer has not enough privileges to seriailze resource",
                    accDeniedExc
                )
            );
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder deletedResourceData(String rscNameStr, long fullSyncTimestamp, long updateId)
    {
        try
        {
            ctrlStltSerializationWriter.writeDeletedResourceData(rscNameStr, fullSyncTimestamp, updateId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder storPoolData(
        StorPool storPool,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeStorPoolData(storPool, fullSyncTimestamp, updateId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ProtoInterComSerializer has not enough privileges to seriailze storage pool",
                    accDeniedExc
                )
            );
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder deletedStorPoolData(
        String storPoolNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeDeletedStorPoolData(storPoolNameStr, fullSyncTimestamp, updateId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder fullSync(
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources,
        long timestamp,
        long updateId
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeFullSync(nodeSet, storPools, resources, timestamp, updateId, baos);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ProtoInterComSerializer has not enough privileges to seriailze full sync",
                    accDeniedExc
                )
            );
            exceptionOccured = true;
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    /*
     * Satellite -> Controller
     */
    @Override
    public CtrlStltSerializerBuilder primaryRequest(String rscName, String rscUuid)
    {
        try
        {
            ctrlStltSerializationWriter.writePrimaryRequest(rscName, rscUuid, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyResourceApplied(
        String resourceName,
        UUID rscUuid,
        Map<StorPool, Long> freeSpaceMap
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeNotifyResourceApplied(resourceName, rscUuid, freeSpaceMap, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyResourceDeleted(
        String nodeName,
        String resourceName,
        UUID rscUuid,
        Map<StorPool, Long> freeSpaceMap
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeNotifyResourceDeleted(nodeName, resourceName, rscUuid, freeSpaceMap, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyVolumeDeleted(
        String nodeName,
        String resourceName,
        int volumeNr,
        UUID vlmUuid
    )
    {
        try
        {
            ctrlStltSerializationWriter.writeNotifyVolumeDeleted(nodeName, resourceName, volumeNr, vlmUuid, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder requestControllerUpdate()
    {
        try
        {
            ctrlStltSerializationWriter.writeRequestNodeUpdate(UUID.randomUUID(), "thisnamedoesnotmatter", baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder requestNodeUpdate(UUID nodeUuid, String nodeName)
    {
        try
        {
            ctrlStltSerializationWriter.writeRequestNodeUpdate(nodeUuid, nodeName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName)
    {
        try
        {
            ctrlStltSerializationWriter.writeRequestResourceDfnUpdate(rscDfnUuid, rscName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName)
    {
        try
        {
            ctrlStltSerializationWriter.writeRequestResourceUpdate(rscUuid, nodeName, rscName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName)
    {
        try
        {
            ctrlStltSerializationWriter.writeRequestStorPoolUpdate(storPoolUuid, storPoolName, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder cryptKey(byte[] masterKey, long timestamp, long updateId)
    {
        try
        {
            ctrlStltSerializationWriter.writeCryptKey(masterKey, timestamp, updateId, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder inProgressSnapshotEvent(SnapshotState snapshotState)
    {
        try
        {
            ctrlStltSerializationWriter.writeInProgressSnapshotEvent(snapshotState, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    public interface CtrlStltSerializationWriter extends CommonSerializerWriter
    {
        /*
         * Controller -> Satellite
         */
        void writeAuthMessage(
            UUID nodeUuid,
            String nodeName,
            byte[] sharedSecret,
            UUID nodeDisklessStorPoolDfnUuid,
            UUID nodeDisklessStorPoolUuid,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeChangedNode(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
            throws IOException;

        void writeChangedResource(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
            throws IOException;

        void writeChangedStorPool(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
            throws IOException;

        void writeControllerData(
            long fullSyncTimestamp,
            long serializerId,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeNodeData(
            Node node,
            Collection<Node> relatedNodes,
            long fullSyncTimestamp,
            long serializerId,
            ByteArrayOutputStream baos
        )
            throws IOException, AccessDeniedException;

        void writeDeletedNodeData(
            String nodeNameStr,
            long fullSyncTimestamp,
            long updateId,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeResourceData(
            Resource localResource,
            long fullSyncTimestamp,
            long updateId,
            ByteArrayOutputStream baos
        )
            throws IOException, AccessDeniedException;

        void writeDeletedResourceData(
            String rscNameStr,
            long fullSyncTimestamp,
            long updateId,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeStorPoolData(
            StorPool storPool,
            long fullSyncTimestamp,
            long updateId,
            ByteArrayOutputStream baos
        )
            throws IOException, AccessDeniedException;

        void writeDeletedStorPoolData(
            String storPoolNameStr,
            long fullSyncTimestamp,
            long updateId,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeFullSync(
            Set<Node> nodeSet,
            Set<StorPool> storPools,
            Set<Resource> resources,
            long timestamp,
            long updateId,
            ByteArrayOutputStream baos
        )
            throws IOException, AccessDeniedException;

        /*
         * Satellite -> Controller
         */
        void writePrimaryRequest(String rscName, String rscUuid, ByteArrayOutputStream baos)
            throws IOException;

        void writeNotifyResourceApplied(
            String resourceName,
            UUID rscUuid,
            Map<StorPool, Long> freeSpaceMap,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeNotifyResourceDeleted(
            String nodeName,
            String resourceName,
            UUID rscUuid,
            Map<StorPool, Long> freeSpaceMap,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeNotifyVolumeDeleted(
            String nodeName,
            String resourceName,
            int volumeNr,
            UUID vlmUuid,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeRequestNodeUpdate(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
            throws IOException;

        void writeRequestResourceDfnUpdate(UUID rscDfnUuid, String rscName, ByteArrayOutputStream baos)
            throws IOException;

        void writeRequestResourceUpdate(
            UUID rscUuid,
            String nodeName,
            String rscName,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeRequestStorPoolUpdate(
            UUID storPoolUuid,
            String storPoolName,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeCryptKey(byte[] masterKey, long timestamp, long updateId, ByteArrayOutputStream baos)
            throws IOException;

        void writeInProgressSnapshotEvent(
            SnapshotState snapshotState,
            ByteArrayOutputStream baos
        )
            throws IOException;
    }
}
