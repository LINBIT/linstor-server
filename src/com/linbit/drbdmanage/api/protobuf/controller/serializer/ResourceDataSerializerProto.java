package com.linbit.drbdmanage.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.controller.interfaces.ResourceDataSerializer;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.Vlm;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.drbdmanage.proto.internal.MsgIntRscChangedOuterClass.MsgIntRscChanged;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

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

            MsgIntRscChanged.newBuilder()
                .setResourceName(rscNameStr)
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
}
