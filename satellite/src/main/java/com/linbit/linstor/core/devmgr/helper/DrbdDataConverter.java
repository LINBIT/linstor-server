package com.linbit.linstor.core.devmgr.helper;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LayerDataFactory;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdVlmDfnData;
import com.linbit.utils.Pair;

import static com.linbit.utils.AccessUtils.execPrivileged;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DrbdDataConverter implements LayerDataConverter
{
    private final AccessContext sysCtx;
    private final LayerDataFactory layerDataFactory;

    @Inject
    public DrbdDataConverter(
        @SystemContext AccessContext sysCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        sysCtx = sysCtxRef;
        layerDataFactory = layerDataFactoryRef;
    }

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public boolean isLayerNeeded(Resource rsc)
    {
        boolean ret;
        // for now: if one volume needs drbd, whole resource gets drbd
        ret = rsc.streamVolumes()
            .anyMatch(
                vlm ->
                execPrivileged(() -> vlm.getStorPool(sysCtx))
                    .getDriverKind()
                    .supportsDrbd()
            );

        return ret;
    }

    @Override
    public Pair<RscLayerObject, List<RscLayerObject>> convert(Resource rsc, RscLayerObject ignored)
        throws AccessDeniedException, InvalidKeyException, SQLException
    {
        ResourceDefinition rscDfn = rsc.getDefinition();
        List<RscLayerObject> drbdChildren = new ArrayList<>();
        List<DrbdRscData> drbdRscDataList = new ArrayList<>();

        DrbdRscDfnData drbdRscDfnData = syncDrbdRscDfnData(rscDfn, drbdRscDataList);
        Map<VolumeNumber, DrbdVlmData> drbdVlmData = new TreeMap<>();
        DrbdRscData drbdRscData = layerDataFactory.createDrbdRscData(
            rsc,
            "",
            null,
            drbdRscDfnData,
            drbdChildren,
            drbdVlmData,
            rsc.getNodeId(),
            rsc.isDiskless(sysCtx),
            rsc.disklessForPeers(sysCtx)
        );

        drbdRscDataList.add(drbdRscData); // local resource

        Map<VolumeNumber, DrbdVlmDfnData> vlmDfnDataMap = new TreeMap<>();

        for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
        {
            short peerSlots;
            {
                String peerSlotsProp = rsc.getProps(sysCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
                // Property is checked when the API sets it; if it still throws for whatever reason,
                // it is logged as an unexpected exception in dispatchResource()
                peerSlots = peerSlotsProp == null ?
                    InternalApiConsts.DEFAULT_PEER_SLOTS :
                    Short.parseShort(peerSlotsProp);
            }

            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            DrbdVlmDfnData drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                vlmDfn,
                vlmDfn.getMinorNr(sysCtx),
                peerSlots
            );
            vlmDfnDataMap.put(vlmDfn.getVolumeNumber(), drbdVlmDfnData);
            drbdVlmData.put(
                vlmDfn.getVolumeNumber(),
                layerDataFactory.createDrbdVlmData(
                    vlm,
                    drbdRscData,
                    // controller should store drbdVlmDfnData within vlmDfn and use the same reference
                    // for all drbdVlmData.
                    drbdVlmDfnData
                )
            );
        }

        for (Resource peerRsc : rsc.getDefinition().streamResource(sysCtx).collect(Collectors.toList()))
        {
            if (!peerRsc.equals(rsc))
            {
                Map<VolumeNumber, DrbdVlmData> peerDrbdVlmDataMap = new TreeMap<>();
                DrbdRscData drbdPeerRscdata = layerDataFactory.createDrbdRscData(
                    peerRsc,
                    "",
                    null,                    // ignore peer's layer stack above drbd
                    drbdRscDfnData,
                    Collections.emptyList(), // ignore peer's layer stack below drbd
                    peerDrbdVlmDataMap,
                    peerRsc.getNodeId(),
                    peerRsc.isDiskless(sysCtx),
                    peerRsc.disklessForPeers(sysCtx)
                );

                for (Volume peerVlm : peerRsc.streamVolumes().collect(Collectors.toList()))
                {
                    VolumeNumber vlmNr = peerVlm.getVolumeDefinition().getVolumeNumber();
                    DrbdVlmDfnData drbdVlmDfnData = vlmDfnDataMap.get(vlmNr);

                    peerDrbdVlmDataMap.put(
                        vlmNr,
                        layerDataFactory.createDrbdVlmData(
                            peerVlm,
                            drbdPeerRscdata,
                            // controller should store drbdVlmDfnData within vlmDfn and use the same reference
                            // for all drbdVlmData.
                            drbdVlmDfnData
                        )
                    );
                }

                drbdRscDataList.add(drbdPeerRscdata); // peer resource
            }
        }

        return new Pair<>(drbdRscData, drbdChildren);
    }

    private DrbdRscDfnData syncDrbdRscDfnData(ResourceDefinition rscDfn, List<DrbdRscData> drbdRscDataList)
        throws AccessDeniedException, SQLException
    {
        DrbdRscDfnData rscDfnData = layerDataFactory.createDrbdRscDfnData(
            rscDfn,
            rscDfn.getPort(sysCtx),
            rscDfn.getTransportType(sysCtx),
            rscDfn.getSecret(sysCtx),
            drbdRscDataList
        );
        rscDfn.setLayerData(sysCtx, rscDfnData);

        return rscDfnData;
    }
}
