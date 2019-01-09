package com.linbit.linstor.core.devmgr.helper;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
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
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.provider.StorageRscData;
import com.linbit.linstor.storage.layer.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.utils.SwordfishConsts;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Singleton
public class StoragetDataConverter implements LayerDataConverter
{
    private final AccessContext sysCtx;
    private final LayerDataFactory layerDataFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StoragetDataConverter(
        @SystemContext AccessContext sysCtxRef,
        LayerDataFactory layerDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        sysCtx = sysCtxRef;
        layerDataFactory = layerDataFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public boolean isLayerNeeded(Resource rscRef)
    {
        return true;
    }

    @Override
    public Pair<RscLayerObject, List<RscLayerObject>> convert(Resource rsc, RscLayerObject parentRscData)
        throws AccessDeniedException, InvalidKeyException
    {
        Map<VolumeNumber, VlmProviderObject> storareVlmDataMap = new TreeMap<>();
        StorageRscData storRscData = layerDataFactory.createStorageRscData(
            parentRscData,
            rsc,
            "",
            storareVlmDataMap
        );

        for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
        {
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            VlmProviderObject vlmData;
            switch (vlm.getStorPool(sysCtx).getDeviceProviderKind())
            {
                case DRBD_DISKLESS:
                    vlmData = layerDataFactory.createDrbdDisklessData(vlm, storRscData);
                    break;
                case LVM:
                    vlmData = layerDataFactory.createLvmData(vlm, storRscData);
                    break;
                case LVM_THIN:
                    vlmData = layerDataFactory.createLvmThinData(vlm, storRscData);
                    break;
                case SWORDFISH_INITIATOR:
                    vlmData = layerDataFactory.createSfInitData(
                        vlm,
                        storRscData,
                        getSfVlmDfnData(vlm)
                    );
                    break;
                case SWORDFISH_TARGET:
                    vlmData = layerDataFactory.createSfTargetData(
                        vlm,
                        storRscData,
                        getSfVlmDfnData(vlm)
                    );
                    break;
                case ZFS:
                    vlmData = layerDataFactory.createZfsData(
                        vlm,
                        storRscData,
                        DeviceProviderKind.ZFS
                    );
                    break;
                case ZFS_THIN:
                    vlmData = layerDataFactory.createZfsData(
                        vlm,
                        storRscData,
                        DeviceProviderKind.ZFS_THIN
                    );
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                    throw new ImplementationError("Cannot create a providerData for a layer type");
                default:
                    throw new ImplementationError(
                        "Unkonwn deviceProviderKind: " +
                        vlm.getStorPool(sysCtx).getDeviceProviderKind()
                    );
            }
            storareVlmDataMap.put(
                vlmDfn.getVolumeNumber(),
                vlmData
            );
        }

        return new Pair<>(storRscData, Collections.emptyList());
    }

    private SfVlmDfnData getSfVlmDfnData(Volume vlm) throws AccessDeniedException, InvalidKeyException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        SfVlmDfnData vlmDfnData = vlmDfn.getLayerData(sysCtx, DeviceLayerKind.STORAGE);
        if (vlmDfnData == null)
        {
            vlmDfnData = new SfVlmDfnData(
                vlmDfn,
                vlmDfn.getProps(sysCtx).getProp(SwordfishConsts.ODATA, ApiConsts.NAMESPC_STORAGE_DRIVER),
                transMgrProvider
            );
            vlmDfn.setLayerData(sysCtx, vlmDfnData);
        }
        return vlmDfnData;
    }
}
