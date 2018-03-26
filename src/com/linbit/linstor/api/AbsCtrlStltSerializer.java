package com.linbit.linstor.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public abstract class AbsCtrlStltSerializer implements CtrlStltSerializer
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext serializerCtx;

    public AbsCtrlStltSerializer(
        final ErrorReporter errReporterRef,
        final AccessContext serializerCtxRef
    )
    {
        errorReporter = errReporterRef;
        serializerCtx = serializerCtxRef;
    }

    @Override
    public Builder builder(String apiCall, int msgId)
    {
        return new BuilderImpl(apiCall, msgId);
    }

    private class BuilderImpl implements CtrlStltSerializer.Builder
    {
        protected final ByteArrayOutputStream baos;
        protected boolean exceptionOccured = false;

        BuilderImpl(String apiCall, int msgId)
        {
            baos = new ByteArrayOutputStream();

            try
            {
                writeHeader(apiCall, msgId, baos);
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc);
                exceptionOccured = true;
            }
        }

        @Override
        public byte[] build()
        {
            byte[] ret;
            if (exceptionOccured)
            {
                ret = new byte[0]; // do not send corrupted data
            }
            else
            {
                ret = baos.toByteArray();
            }
            return ret;
        }

        /*
         * Controller -> Satellite
         */
        @Override
        public Builder authMessage(
            UUID nodeUuid,
            String nodeName,
            byte[] sharedSecret,
            UUID nodeDisklessStorPoolDfnUuid,
            UUID nodeDisklessStorPoolUuid
        )
        {
            try
            {
                writeAuthMessage(
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
        public Builder changedNode(UUID nodeUuid, String nodeName)
        {
            try
            {
                writeChangedNode(nodeUuid, nodeName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder changedResource(UUID rscUuid, String rscName)
        {
            try
            {
                writeChangedResource(rscUuid, rscName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder changedStorPool(UUID storPoolUuid, String storPoolName)
        {
            try
            {
                writeChangedStorPool(storPoolUuid, storPoolName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder nodeData(
            Node node,
            Collection<Node> relatedNodes,
            long fullSyncTimestamp,
            long serializerId
        )
        {
            try
            {
                writeNodeData(node, relatedNodes, fullSyncTimestamp, serializerId, baos);
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
            catch (InvalidNameException invalidNameExc)
            {
                errorReporter.reportError(
                    new ImplementationError(
                        "Invalid name: " + invalidNameExc.invalidName,
                        invalidNameExc
                    )
                );
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder deletedNodeData(
            String nodeNameStr,
            long fullSyncTimestamp,
            long updateId
        )
        {
            try
            {
                writeDeletedNodeData(nodeNameStr, fullSyncTimestamp, updateId, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder resourceData(
            Resource localResource,
            long fullSyncTimestamp,
            long updateId
        )
        {
            try
            {
                writeResourceData(localResource, fullSyncTimestamp, updateId, baos);
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
        public Builder deletedResourceData(String rscNameStr, long fullSyncTimestamp, long updateId)
        {
            try
            {
                writeDeletedResourceData(rscNameStr, fullSyncTimestamp, updateId, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder storPoolData(
            StorPool storPool,
            long fullSyncTimestamp,
            long updateId
        )
        {
            try
            {
                writeStorPoolData(storPool, fullSyncTimestamp, updateId, baos);
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
        public Builder deletedStorPoolData(
            String storPoolNameStr,
            long fullSyncTimestamp,
            long updateId
        )
        {
            try
            {
                writeDeletedStorPoolData(storPoolNameStr, fullSyncTimestamp, updateId, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder fullSync(
            Set<Node> nodeSet,
            Set<StorPool> storPools,
            Set<Resource> resources,
            long timestamp,
            long updateId
        )
        {
            try
            {
                writeFullSync(nodeSet, storPools, resources, timestamp, updateId, baos);
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
            catch (InvalidNameException invalidNameExc)
            {
                errorReporter.reportError(
                    new ImplementationError(
                        "Invalid name: " + invalidNameExc.invalidName,
                        invalidNameExc
                    )
                );
                exceptionOccured = true;
            }
            return this;
        }

        /*
         * Satellite -> Controller
         */
        @Override
        public Builder primaryRequest(String rscName, String rscUuid)
        {
            try
            {
                writePrimaryRequest(rscName, rscUuid, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder resourceState(String nodeName, ResourceState rsc)
        {
            try
            {
                writeResourceState(nodeName, rsc, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder notifyResourceApplied(
            String resourceName,
            UUID rscUuid,
            Map<StorPool, Long> freeSpaceMap
        )
        {
            try
            {
                writeNotifyResourceApplied(resourceName, rscUuid, freeSpaceMap, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder notifyResourceDeleted(
            String nodeName,
            String resourceName,
            UUID rscUuid,
            Map<StorPool, Long> freeSpaceMap
        )
        {
            try
            {
                writeNotifyResourceDeleted(nodeName, resourceName, rscUuid, freeSpaceMap, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder notifyVolumeDeleted(
            String nodeName,
            String resourceName,
            int volumeNr,
            UUID vlmUuid
        )
        {
            try
            {
                writeNotifyVolumeDeleted(nodeName, resourceName, volumeNr, vlmUuid, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder requestNodeUpdate(UUID nodeUuid, String nodeName)
        {
            try
            {
                writeRequestNodeUpdate(nodeUuid, nodeName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder requestResourceDfnUpdate(UUID rscDfnUuid, String rscName)
        {
            try
            {
                writeRequestResourceDfnUpdate(rscDfnUuid, rscName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName)
        {
            try
            {
                writeRequestResourceUpdate(rscUuid, nodeName, rscName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName)
        {
            try
            {
                writeRequestStorPoolUpdate(storPoolUuid, storPoolName, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder cryptKey(byte[] masterKey, long timestamp, long updateId)
        {
            try
            {
                writeCryptKey(masterKey, timestamp, updateId, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

    }

    public abstract void writeHeader(String apiCall, int msgId, ByteArrayOutputStream baos)
        throws IOException;

    /*
     * Controller -> Satellite
     */
    public abstract void writeAuthMessage(
        UUID nodeUuid,
        String nodeName,
        byte[] sharedSecret,
        UUID nodeDisklessStorPoolDfnUuid,
        UUID nodeDisklessStorPoolUuid,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeChangedNode(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeChangedResource(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeChangedStorPool(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeNodeData(
        Node node,
        Collection<Node> relatedNodes,
        long fullSyncTimestamp,
        long serializerId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException, InvalidNameException;

    public abstract void writeDeletedNodeData(
        String nodeNameStr,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeResourceData(
        Resource localResource,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException;

    public abstract void writeDeletedResourceData(
        String rscNameStr,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeStorPoolData(
        StorPool storPool,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException;

    public abstract void writeDeletedStorPoolData(
        String storPoolNameStr,
        long fullSyncTimestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeFullSync(
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources,
        long timestamp,
        long updateId,
        ByteArrayOutputStream baos
    )
        throws IOException, AccessDeniedException, InvalidNameException;

    /*
     * Satellite -> Controller
     */
    public abstract void writePrimaryRequest(String rscName, String rscUuid, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeResourceState(
        String nodeName,
        ResourceState rscState,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeNotifyResourceApplied(
        String resourceName,
        UUID rscUuid,
        Map<StorPool, Long> freeSpaceMap,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeNotifyResourceDeleted(
        String nodeName,
        String resourceName,
        UUID rscUuid,
        Map<StorPool, Long> freeSpaceMap,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeNotifyVolumeDeleted(
        String nodeName,
        String resourceName,
        int volumeNr,
        UUID vlmUuid,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeRequestNodeUpdate(UUID nodeUuid, String nodeName, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeRequestResourceDfnUpdate(UUID rscDfnUuid, String rscName, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeRequestResourceUpdate(
        UUID rscUuid,
        String nodeName,
        String rscName,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeRequestStorPoolUpdate(
        UUID storPoolUuid,
        String storPoolName,
        ByteArrayOutputStream baos
    )
        throws IOException;

    public abstract void writeCryptKey(byte[] masterKey, long timestamp, long updateId, ByteArrayOutputStream baos)
        throws IOException;

}
