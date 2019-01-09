package com.linbit.linstor.core.devmgr.helper;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LayerDataFactory;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.adapter.cryptsetup.CryptSetupRscStltData;
import com.linbit.linstor.storage.layer.adapter.cryptsetup.CryptSetupVlmStltData;
import com.linbit.utils.Pair;

import static com.linbit.utils.AccessUtils.execPrivileged;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Singleton
public class CryptDataConverter implements LayerDataConverter
{
    private final AccessContext sysCtx;
    private final LayerDataFactory layerDataFactory;

    @Inject
    public CryptDataConverter(
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
        return DeviceLayerKind.CRYPT_SETUP;
    }

    @Override
    public boolean isLayerNeeded(Resource rsc)
    {
        boolean ret;
        // known limitation: if one volume definition wants to crypt, all volume definitions get crypt
        ret = rsc.streamVolumes()
            .anyMatch(
                vlm -> execPrivileged(() -> vlm.getVolumeDefinition().getFlags().isSet(sysCtx, VlmDfnFlags.ENCRYPTED))
            );
        return ret;
    }

    @Override
    public Pair<RscLayerObject, List<RscLayerObject>> convert(Resource rsc, RscLayerObject parentRscData)
        throws AccessDeniedException, InvalidKeyException
    {
        List<RscLayerObject> cryptRscDataChildren = new ArrayList<>();
        Map<VolumeNumber, CryptSetupVlmStltData> cryptVlmMap = new TreeMap<>();
        CryptSetupRscStltData cryptRscData = layerDataFactory.createCryptSetupRscData(
            rsc,
            "",
            parentRscData,
            cryptRscDataChildren,
            cryptVlmMap
        );
        for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
        {
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            cryptVlmMap.put(
                vlmDfn.getVolumeNumber(),
                layerDataFactory.createCryptSetupVlmData(
                    vlm,
                    cryptRscData,
                    vlmDfn.getCryptKey(sysCtx).getBytes()
                )
            );
        }

        return new Pair<>(cryptRscData, cryptRscDataChildren);
    }

}
