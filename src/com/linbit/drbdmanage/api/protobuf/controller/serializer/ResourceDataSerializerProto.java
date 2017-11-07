package com.linbit.drbdmanage.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.google.protobuf.ByteString;
import com.linbit.ImplementationError;
import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.controller.interfaces.ResourceDataSerializer;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.Vlm;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherResourceData;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscIdOuterClass.MsgIntRscId;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class ResourceDataSerializerProto implements ResourceDataSerializer
{
    private AccessContext serializerCtx;
    private ErrorReporter errorReporter;

    public ResourceDataSerializerProto(AccessContext serializerCtxRef, ErrorReporter errorReporterRef)
    {
        serializerCtx = serializerCtxRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public byte[] getChangedMessage(ResourceData rsc)
    {
        byte[] ret = null;

        String rscNameStr = rsc.getDefinition().getName().value;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try
        {
            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_RSC_CHANGED)
                .setMsgId(0) // TODO: change to something that defines this message as a protobuf msg
                .build()
                .writeDelimitedTo(baos);

            MsgIntRscId.newBuilder()
                .setResourceName(rscNameStr)
                .setUuid(ByteString.copyFrom(UuidUtils.asByteArray(rsc.getUuid())))
                .build()
                .writeDelimitedTo(baos);

            ret = baos.toByteArray();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return ret;
    }

    @Override
    public byte[] serialize(ResourceData rsc)
    {
        byte[] ret = null;

        Iterator<Resource> rscIterator;
        try
        {
            rscIterator = rsc.getDefinition().iterateResource(serializerCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();

                List<Vlm> vlms = new ArrayList<>();

                Iterator<Volume> vlmIterator = rsc.iterateVolumes();
                while (vlmIterator.hasNext())
                {
                    Volume currenctVlm = vlmIterator.next();
                    vlms.add(
                        Vlm.newBuilder()
                        .setBlockDevice(currenctVlm.getBlockDevicePath(serializerCtx))
                        .setMetaDisk(currenctVlm.getMetaDiskPath(serializerCtx))
                        .setStorPoolName(currenctVlm.getStorPool(serializerCtx).getName().value)
                        .setVlmNr(currenctVlm.getVolumeDefinition().getVolumeNumber(serializerCtx).value)
                        .build()
                    );
                }

                Map<String, String> props = new TreeMap<>();
                props.put(ApiConsts.KEY_NODE_ID, Integer.toString(rsc.getNodeId().value));
                MsgCrtRsc crtRsc = MsgCrtRsc.newBuilder()
                    .setNodeName(rsc.getAssignedNode().getName().value)
                    .setRscName(rsc.getDefinition().getName().value)
                    .addAllRscProps(BaseProtoApiCall.asLinStorMapEntryList(props))
                    .addAllVlms(vlms)
                    .build();
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Access denied for ResourceDataSerializer",
                    accDeniedExc
                )
            );
        }
        return ret;
    }

    @Override
    public byte[] getRscReqResponse(
        int msgId,
        Resource localResource,
        List<Resource> otherResources
    )
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            ResourceDefinition rscDfn = localResource.getDefinition();
            String rscName = rscDfn.getName().displayValue;
            byte[] rscDfnUuid = UuidUtils.asByteArray(rscDfn.getUuid());
            Map<String, String> rscDfnProps = rscDfn.getProps(serializerCtx).map();
            byte[] rscUuid = UuidUtils.asByteArray(localResource.getUuid());
            Map<String, String> rscProps = localResource.getProps(serializerCtx).map();

            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_RSC_DATA)
                .setMsgId(msgId)
                .build()
                .writeDelimitedTo(baos);

            MsgIntRscData.newBuilder()
                .setResourceName(rscName)
                .setResourceDfnUuid(ByteString.copyFrom(rscDfnUuid))
                .addAllRscDfnProps(BaseProtoApiCall.asLinStorMapEntryList(rscDfnProps))
                .setResourceUuid(ByteString.copyFrom(rscUuid))
                .addAllLocalRscProps(BaseProtoApiCall.asLinStorMapEntryList(rscProps))
                .addAllLocalVolumes(
                    buildVlmMessages(localResource)
                )
                .addAllOtherResources(
                    buildOtherResources(otherResources)
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Access denied for ResourceDataSerializer",
                    accDeniedExc
                )
            );
        }
        return baos.toByteArray();
    }

    private List<Vlm> buildVlmMessages(Resource localResource)
        throws AccessDeniedException
    {
        List<Vlm> vlmList = new ArrayList<>();

        Iterator<Volume> localVolIterator = localResource.iterateVolumes();
        while (localVolIterator.hasNext())
        {
            Volume vol = localVolIterator.next();
            Map<String, String> volProps = vol.getProps(serializerCtx).map();
            vlmList.add(
                Vlm.newBuilder()
                    .setVlmNr(vol.getVolumeDefinition().getVolumeNumber(serializerCtx).value)
                    .setBlockDevice(vol.getBlockDevicePath(serializerCtx))
                    .setMetaDisk(vol.getMetaDiskPath(serializerCtx))
                    .setStorPoolName(vol.getStorPool(serializerCtx).getName().displayValue)
                    .addAllVlmProps(BaseProtoApiCall.asLinStorMapEntryList(volProps))
                    .build()
            );
        }
        return vlmList;
    }

    private List<MsgIntOtherResourceData> buildOtherResources(List<Resource> otherResources)
        throws AccessDeniedException
    {
        List<MsgIntOtherResourceData> list = new ArrayList<>();

        for (Resource rsc : otherResources)
        {
            Node node = rsc.getAssignedNode();
            byte[] nodeUuid = UuidUtils.asByteArray(node.getUuid());
            Map<String, String> nodeProps = node.getProps(serializerCtx).map();
            byte[] rscUuid = UuidUtils.asByteArray(rsc.getUuid());
            Map<String, String> rscProps = rsc.getProps(serializerCtx).map();
            list.add(
                MsgIntOtherResourceData.newBuilder()
                    .setNodeName(node.getName().displayValue)
                    .setNodeUuid(ByteString.copyFrom(nodeUuid))
                    .addAllNodeProps(BaseProtoApiCall.asLinStorMapEntryList(nodeProps))
                    .setResourceUuid(ByteString.copyFrom(rscUuid))
                    .addAllRscProps(BaseProtoApiCall.asLinStorMapEntryList(rscProps))
                    .addAllLocalVolumes(
                        buildVlmMessages(rsc)
                    )
                    .build()
            );
        }

        return list;
    }
}
