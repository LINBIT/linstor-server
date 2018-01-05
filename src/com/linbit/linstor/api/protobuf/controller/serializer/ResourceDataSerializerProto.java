package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class ResourceDataSerializerProto extends AbsSerializerProto<Resource>
{
    public ResourceDataSerializerProto(AccessContext serializerCtxRef, ErrorReporter errorReporterRef)
    {
        super(
            serializerCtxRef,
            errorReporterRef,
            InternalApiConsts.API_CHANGED_RSC,
            InternalApiConsts.API_APPLY_RSC
        );
    }

    @Override
    protected String getName(Resource rsc)
    {
        return rsc.getDefinition().getName().displayValue;
    }

    @Override
    protected UUID getUuid(Resource rsc)
    {
        return rsc.getUuid();
    }

    @Override
    protected MsgIntRscData buildData(Resource localResource) throws IOException, AccessDeniedException
    {
        List<Resource> otherResources = new ArrayList<>();
        Iterator<Resource> rscIterator = localResource.getDefinition().iterateResource(serializerCtx);
        while (rscIterator.hasNext())
        {
            Resource rsc = rscIterator.next();
            if (!rsc.equals(localResource))
            {
                otherResources.add(rsc);
            }
        }

        ResourceDefinition rscDfn = localResource.getDefinition();
        String rscName = rscDfn.getName().displayValue;
        Map<String, String> rscDfnProps = rscDfn.getProps(serializerCtx).map();
        Map<String, String> rscProps = localResource.getProps(serializerCtx).map();

        return MsgIntRscData.newBuilder()
            .setRscName(rscName)
            .setRscDfnUuid(rscDfn.getUuid().toString())
            .setRscDfnPort(rscDfn.getPort(serializerCtx).value)
            .setRscDfnFlags(rscDfn.getFlags().getFlagsBits(serializerCtx))
            .setRscDfnSecret(rscDfn.getSecret(serializerCtx))
            .addAllRscDfnProps(BaseProtoApiCall.fromMap(rscDfnProps))
            .setLocalRscUuid(localResource.getUuid().toString())
            .setLocalRscFlags(localResource.getStateFlags().getFlagsBits(serializerCtx))
            .setLocalRscNodeId(localResource.getNodeId().value)
            .addAllLocalRscProps(BaseProtoApiCall.fromMap(rscProps))
            .addAllVlmDfns(
                buildVlmDfnMessages(localResource)
            )
            .addAllLocalVolumes(
                buildVlmMessages(localResource)
            )
            .addAllOtherResources(
                buildOtherResources(otherResources)
            )
            .setRscDfnTransportType(rscDfn.getTransportType(serializerCtx).name())
            .build();
    }

    private Iterable<? extends VlmDfn> buildVlmDfnMessages(Resource localResource)
        throws AccessDeniedException
    {
        List<VlmDfn> list = new ArrayList<>();

        Iterator<Volume> localVolIterator = localResource.iterateVolumes();
        while (localVolIterator.hasNext())
        {
            Volume vol = localVolIterator.next();
            VolumeDefinition vlmDfn = vol.getVolumeDefinition();

            Map<String, String> vlmDfnProps = vlmDfn.getProps(serializerCtx).map();
            list.add(
                VlmDfn.newBuilder()
                    .setVlmDfnUuid(vlmDfn.getUuid().toString())
                    .setVlmNr(vlmDfn.getVolumeNumber().value)
                    .setVlmSize(vlmDfn.getVolumeSize(serializerCtx))
                    .setVlmMinor(vlmDfn.getMinorNr(serializerCtx).value)
                    .addAllVlmFlags(Volume.VlmFlags.toStringList(vlmDfn.getFlags().getFlagsBits(serializerCtx)))
                    .addAllVlmProps(BaseProtoApiCall.fromMap(vlmDfnProps))
                    .build()
            );
        }

        return list;
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
            StorPool vlmStorPool = vol.getStorPool(serializerCtx);
            Vlm.Builder builder = Vlm.newBuilder()
                .setVlmDfnUuid(vol.getVolumeDefinition().getUuid().toString())
                .setVlmUuid(vol.getUuid().toString())
                .setVlmNr(vol.getVolumeDefinition().getVolumeNumber().value)
                .addAllVlmFlags(Volume.VlmFlags.toStringList(vol.getFlags().getFlagsBits(serializerCtx)))
                .setStorPoolUuid(vlmStorPool.getUuid().toString())
                .setStorPoolName(vlmStorPool.getName().displayValue)
                .addAllVlmProps(BaseProtoApiCall.fromMap(volProps));
            String blockDev = vol.getBlockDevicePath(serializerCtx);
            if (blockDev != null)
            {
                builder.setBlockDevice(blockDev);
            }
            String metaDisk = vol.getMetaDiskPath(serializerCtx);
            if (metaDisk != null)
            {
                builder.setMetaDisk(metaDisk);
            }
            vlmList.add(builder.build());
        }
        return vlmList;
    }

    private List<MsgIntOtherRscData> buildOtherResources(List<Resource> otherResources)
        throws AccessDeniedException
    {
        List<MsgIntOtherRscData> list = new ArrayList<>();

        for (Resource rsc : otherResources)
        {
            Node node = rsc.getAssignedNode();
            Map<String, String> rscProps = rsc.getProps(serializerCtx).map();
            list.add(
                MsgIntOtherRscData.newBuilder()
                    .setNode(buildOtherNode(node))
                    .setNodeFlags(node.getFlags().getFlagsBits(serializerCtx))
                    .setRscUuid(rsc.getUuid().toString())
                    .setRscNodeId(rsc.getNodeId().value)
                    .setRscFlags(rsc.getStateFlags().getFlagsBits(serializerCtx))
                    .addAllRscProps(BaseProtoApiCall.fromMap(rscProps))
                    .addAllLocalVlms(
                        buildVlmMessages(rsc)
                    )
                    .build()
            );
        }

        return list;
    }

    private NodeOuterClass.Node buildOtherNode(Node node) throws AccessDeniedException
    {
        Map<String, String> nodeProps = node.getProps(serializerCtx).map();
        return NodeOuterClass.Node.newBuilder()
            .setUuid(node.getUuid().toString())
            .setName(node.getName().displayValue)
            .setType(node.getNodeType(serializerCtx).name())
            .addAllProps(BaseProtoApiCall.fromMap(nodeProps))
            .addAllNetInterfaces(buildNodeNetInterfaces(node))
            .build();
    }

    private Iterable<? extends NetInterfaceOuterClass.NetInterface> buildNodeNetInterfaces(Node node)
        throws AccessDeniedException
    {
        List<NetInterfaceOuterClass.NetInterface> protoNetIfs = new ArrayList<>();

        Iterator<NetInterface> netIfs = node.iterateNetInterfaces(serializerCtx);
        while (netIfs.hasNext())
        {
            NetInterface netIf = netIfs.next();

            protoNetIfs.add(
                NetInterfaceOuterClass.NetInterface.newBuilder()
                    .setUuid(netIf.getUuid().toString())
                    .setName(netIf.getName().displayValue)
                    .setAddress(netIf.getAddress(serializerCtx).getAddress())
                    .build()
            );
        }

        return protoNetIfs;
    }
}
