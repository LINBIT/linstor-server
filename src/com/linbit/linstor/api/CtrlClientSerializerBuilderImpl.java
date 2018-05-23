package com.linbit.linstor.api;

import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer.CtrlClientSerializerBuilder;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CtrlClientSerializerBuilderImpl extends CommonSerializerBuilderImpl implements CtrlClientSerializerBuilder
{
    private CtrlClientSerializationWriter serializationWriter;

    public CtrlClientSerializerBuilderImpl(
        ErrorReporter errorReporterRef,
        CtrlClientSerializationWriter serializationWriterRef,
        String apiCall,
        Integer msgId
    )
    {
        super(errorReporterRef, serializationWriterRef, apiCall, msgId);

        serializationWriter = serializationWriterRef;
    }

    @Override
    public CtrlClientSerializerBuilder nodeList(List<Node.NodeApi> nodes)
    {
        try
        {
            serializationWriter.writeNodeList(nodes, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder storPoolDfnList(List<StorPoolDefinition.StorPoolDfnApi> storPoolDfns)
    {
        try
        {
            serializationWriter.writeStorPoolDfnList(storPoolDfns, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder storPoolList(List<StorPool.StorPoolApi> storPools)
    {
        try
        {
            serializationWriter.writeStorPoolList(storPools, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder resourceDfnList(List<ResourceDefinition.RscDfnApi> rscDfns)
    {
        try
        {
            serializationWriter.writeRscDfnList(rscDfns, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder resourceList(
        List<Resource.RscApi> rscs, Map<NodeName, SatelliteState> satelliteStates)
    {
        try
        {
            serializationWriter.writeRscList(rscs, satelliteStates, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder apiVersion(final long features, final String controllerInfo)
    {
        try
        {
            serializationWriter.writeApiVersion(features, controllerInfo, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder ctrlCfgSingleProp(String namespace, String key, String value)
    {
        try
        {
            serializationWriter.writeCtrlCfgSingleProp(namespace, key, value, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder ctrlCfgProps(Map<String, String> map)
    {
        try
        {
            serializationWriter.writeCtrlCfgProps(map, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder snapshotDeploymentEvent(ApiCallRc apiCallRc)
    {
        try
        {
            serializationWriter.writeSnapshotDeploymentEvent(apiCallRc, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    public interface CtrlClientSerializationWriter extends CommonSerializerWriter
    {
        void writeNodeList(List<Node.NodeApi> nodes, ByteArrayOutputStream baos)
            throws IOException;

        void writeStorPoolDfnList(List<StorPoolDefinition.StorPoolDfnApi> storPoolDfns, ByteArrayOutputStream baos)
            throws IOException;

        void writeStorPoolList(List<StorPool.StorPoolApi> storPools, ByteArrayOutputStream baos)
            throws IOException;

        void writeRscDfnList(List<ResourceDefinition.RscDfnApi> rscDfns, ByteArrayOutputStream baos)
            throws IOException;

        void writeRscList(
            List<Resource.RscApi> rscs,
            Map<NodeName, SatelliteState> satelliteStates,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeApiVersion(long features, String controllerInfo, ByteArrayOutputStream baos)
            throws IOException;

        void writeCtrlCfgSingleProp(String namespace, String key, String value, ByteArrayOutputStream baos)
            throws IOException;

        void writeCtrlCfgProps(Map<String, String> map, ByteArrayOutputStream baos)
            throws IOException;

        void writeSnapshotDeploymentEvent(ApiCallRc apiCallRc, ByteArrayOutputStream baos)
            throws IOException;
    }
}
