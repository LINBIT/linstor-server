package com.linbit.linstor.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.linbit.linstor.Node.NodeApi;
import com.linbit.linstor.Resource.RscApi;
import com.linbit.linstor.ResourceDefinition.RscDfnApi;
import com.linbit.linstor.StorPool.StorPoolApi;
import com.linbit.linstor.StorPoolDefinition.StorPoolDfnApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgApiVersionOuterClass;
import com.linbit.linstor.security.AccessContext;

public abstract class AbsCtrlClientSerializer implements CtrlClientSerializer
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext serializerCtx;

    public AbsCtrlClientSerializer(
        final ErrorReporter errReporter,
        final AccessContext serializerCtx
    )
    {
        this.errorReporter = errReporter;
        this.serializerCtx = serializerCtx;
    }

    @Override
    public Builder builder(String apiCall, int msgId)
    {
        return new BuilderImpl(apiCall, msgId);
    }

    private class BuilderImpl implements CtrlClientSerializer.Builder
    {
        protected final ByteArrayOutputStream baos;
        protected boolean exceptionOccured = false;

        public BuilderImpl(String apiCall, int msgId)
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

        @Override
        public Builder nodeList(List<NodeApi> nodes)
        {
            try
            {
                writeNodeList(nodes, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder storPoolDfnList(List<StorPoolDfnApi> storPoolDfns)
        {
            try
            {
                writeStorPoolDfnList(storPoolDfns, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder storPoolList(List<StorPoolApi> storPools)
        {
            try
            {
                writeStorPoolList(storPools, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder resourceDfnList(List<RscDfnApi> rscDfns)
        {
            try
            {
                writeRscDfnList(rscDfns, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder resourceList(List<RscApi> rscs, Collection<ResourceState> rscStates)
        {
            try
            {
                writeRscList(rscs, rscStates, baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
                exceptionOccured = true;
            }
            return this;
        }

        @Override
        public Builder apiVersion(final long features, final String controllerInfo)
        {
            try
            {
                writeApiVersion(features, controllerInfo, baos);
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

    public abstract void writeNodeList(List<NodeApi> nodes, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeStorPoolDfnList(List<StorPoolDfnApi> storPoolDfns, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeStorPoolList(List<StorPoolApi> storPools, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeRscDfnList(List<RscDfnApi> rscDfns, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeRscList(List<RscApi> rscs, Collection<ResourceState> rscStates, ByteArrayOutputStream baos)
        throws IOException;

    public abstract void writeApiVersion(final long features, final String controllerInfo, ByteArrayOutputStream baos)
        throws IOException;
}
