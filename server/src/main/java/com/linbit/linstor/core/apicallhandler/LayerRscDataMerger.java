package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishInitiatorVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishVlmDfnPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.SQLException;

@Singleton
public class LayerRscDataMerger
{
    private final AccessContext apiCtx;
    private final LayerDataFactory layerDataFactory;

    @Inject
    public LayerRscDataMerger(
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        apiCtx = apiCtxRef;
        layerDataFactory = layerDataFactoryRef;
    }

    private interface RscDataExtractor
    {
        RscLayerObject restore(
            Resource rsc,
            RscLayerDataApi rscLayerDataPojo,
            RscLayerObject parent
        )
            throws SQLException, ValueOutOfRangeException, AccessDeniedException, IllegalArgumentException,
                ExhaustedPoolException, ValueInUseException;
    }

    public void restoreLayerData(
        Resource rsc,
        RscLayerDataApi rscLayerDataPojo
    )
    {
        try
        {
            restore(rsc, rscLayerDataPojo, null);
        }
        catch (AccessDeniedException | SQLException | ValueOutOfRangeException | IllegalArgumentException |
            ExhaustedPoolException | ValueInUseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void restore(
        Resource rsc,
        RscLayerDataApi rscLayerDataPojo,
        RscLayerObject parent
    )
        throws AccessDeniedException, SQLException, IllegalArgumentException,
            ImplementationError, ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException
    {
        RscDataExtractor rscRestorer;
        switch (rscLayerDataPojo.getLayerKind())
        {
            case DRBD:
                rscRestorer = this::restoreDrbdRscData;
                break;
            case LUKS:
                rscRestorer = this::restoreLuksRscData;
                break;
            case STORAGE:
                rscRestorer = this::restoreStorageRscData;
                break;
            case NVME:
                rscRestorer = this::restoreNvmeRscData;
                break;
            default:
                throw new ImplementationError("Unexpected layer kind: " + rscLayerDataPojo.getLayerKind());
        }
        RscLayerObject rscLayerObject = rscRestorer.restore(rsc, rscLayerDataPojo, parent);

        for (RscLayerDataApi childRscPojo : rscLayerDataPojo.getChildren())
        {
            restore(rsc, childRscPojo, rscLayerObject);
        }
    }

    private DrbdRscData restoreDrbdRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent
    )
        throws SQLException, ValueOutOfRangeException, AccessDeniedException, IllegalArgumentException,
            ExhaustedPoolException, ValueInUseException
    {
        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscDataPojo;

        DrbdRscDfnData drbdRscDfnData = restoreDrbdRscDfn(rsc.getDefinition(), drbdRscPojo.getDrbdRscDfn());

        DrbdRscData drbdRscData = null;
        if (parent == null)
        {
            drbdRscData = rsc.getLayerData(apiCtx);
        }
        else
        {
            drbdRscData = findChild(parent, rscDataPojo.getId());
        }

        if (drbdRscData == null)
        {
            drbdRscData = layerDataFactory.createDrbdRscData(
                rscDataPojo.getId(),
                rsc,
                rscDataPojo.getRscNameSuffix(),
                parent,
                drbdRscDfnData,
                new NodeId(drbdRscPojo.getNodeId()),
                drbdRscPojo.getPeerSlots(),
                drbdRscPojo.getAlStripes(),
                drbdRscPojo.getAlStripeSize(),
                drbdRscPojo.getFlags()
            );
            drbdRscDfnData.getDrbdRscDataList().add(drbdRscData);
            if (parent == null)
            {
                rsc.setLayerData(apiCtx, drbdRscData);
            }
            else
            {
                parent.getChildren().add(drbdRscData);
            }
        }
        else
        {
            drbdRscData.getFlags().resetFlagsTo(
                apiCtx,
                DrbdRscFlags.restoreFlags(drbdRscPojo.getFlags())
            );
            updateChildsParent(drbdRscData, parent);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (DrbdVlmPojo drbdVlmPojo : drbdRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(drbdVlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                drbdRscData.remove(vlmNr);
            }
            else
            {
                restoreDrbdVlm(vlm, drbdRscData, drbdVlmPojo);
            }
        }
        return drbdRscData;
    }

    private DrbdRscDfnData restoreDrbdRscDfn(
        ResourceDefinition rscDfn,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, SQLException, ValueOutOfRangeException, AccessDeniedException,
            ExhaustedPoolException, ValueInUseException
    {
        DrbdRscDfnData rscDfnData = rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
        if (rscDfnData == null)
        {
            rscDfnData = layerDataFactory.createDrbdRscDfnData(
                rscDfn,
                drbdRscDfnPojo.getRscNameSuffix(),
                drbdRscDfnPojo.getPeerSlots(),
                drbdRscDfnPojo.getAlStripes(),
                drbdRscDfnPojo.getAlStripeSize(),
                drbdRscDfnPojo.getPort(),
                TransportType.valueOfIgnoreCase(drbdRscDfnPojo.getTransportType(), TransportType.IP),
                drbdRscDfnPojo.getSecret()
            );
            rscDfn.setLayerData(apiCtx, rscDfnData);
        }
        else
        {
            rscDfnData.setPort(new TcpPortNumber(drbdRscDfnPojo.getPort()));
            rscDfnData.setSecret(drbdRscDfnPojo.getSecret());
            rscDfnData.setTransportType(
                TransportType.valueOfIgnoreCase(
                    drbdRscDfnPojo.getTransportType(),
                    TransportType.IP
                )
            );
        }
        return rscDfnData;
    }

    private void restoreDrbdVlm(Volume vlm, DrbdRscData rscData, DrbdVlmPojo vlmPojo)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        DrbdVlmDfnData drbdVlmDfnData = restoreDrbdVlmDfn(vlmDfn, vlmPojo.getDrbdVlmDfn());

        DrbdVlmData drbdVlmData = rscData.getVlmLayerObjects().get(vlmNr);
        if (drbdVlmData == null)
        {
            drbdVlmData = layerDataFactory.createDrbdVlmData(
                vlm,
                rscData,
                drbdVlmDfnData
            );
            rscData.getVlmLayerObjects().put(vlmNr, drbdVlmData);
        }
        else
        {
            drbdVlmData.setAllocatedSize(vlmPojo.getAllocatedSize());
            drbdVlmData.setDevicePath(vlmPojo.getDevicePath());
            drbdVlmData.setDiskState(vlmPojo.getDiskState());
            drbdVlmData.setMetaDiskPath(vlmPojo.getMetaDisk());
            drbdVlmData.setUsableSize(vlmPojo.getUsableSize());
        }
    }

    private DrbdVlmDfnData restoreDrbdVlmDfn(VolumeDefinition vlmDfn, DrbdVlmDfnPojo drbdVlmDfnPojo)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        DrbdVlmDfnData drbdVlmDfnData = vlmDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
        if (drbdVlmDfnData == null)
        {
            drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                vlmDfn,
                drbdVlmDfnPojo.getRscNameSuffix(),
                drbdVlmDfnPojo.getMinorNr(),
                vlmDfn.getResourceDefinition().getLayerData(apiCtx, DeviceLayerKind.DRBD)
            );
            vlmDfn.setLayerData(apiCtx, drbdVlmDfnData);
        }
        else
        {
            // nothing to merge
        }
        return drbdVlmDfnData;
    }

    private LuksRscData restoreLuksRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException
    {
        LuksRscPojo luksRscPojo = (LuksRscPojo) rscDataPojo;

        LuksRscData luksRscData = null;
        if (parent == null)
        {
            luksRscData = rsc.getLayerData(apiCtx);
        }
        else
        {
            luksRscData = findChild(parent, rscDataPojo.getId());
        }

        if (luksRscData == null)
        {
            luksRscData = layerDataFactory.createLuksRscData(
                luksRscPojo.getId(),
                rsc,
                luksRscPojo.getRscNameSuffix(),
                parent
            );
            if (parent == null)
            {
                rsc.setLayerData(apiCtx, luksRscData);
            }
            else
            {
                updateChildsParent(luksRscData, parent);
            }
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (LuksVlmPojo luksVlmPojo : luksRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(luksVlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                luksRscData.remove(vlmNr);
            }
            else
            {
                restoreLuksVlm(vlm, luksRscData, luksVlmPojo);
            }
        }
        return luksRscData;
    }

    private void restoreLuksVlm(
        Volume vlm,
        LuksRscData luksRscData,
        LuksVlmPojo vlmPojo
    )
        throws SQLException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        LuksVlmData luksVlmData = luksRscData.getVlmLayerObjects().get(vlmNr);
        if (luksVlmData == null)
        {
            luksVlmData = layerDataFactory.createLuksVlmData(
                vlm,
                luksRscData,
                vlmPojo.getEncryptedPassword()
            );
            luksRscData.getVlmLayerObjects().put(vlmNr, luksVlmData);
        }
        else
        {
            luksVlmData.setAllocatedSize(vlmPojo.getAllocatedSize());
            luksVlmData.setBackingDevice(vlmPojo.getBackingDevice());
            luksVlmData.setDevicePath(vlmPojo.getDevicePath());
            luksVlmData.setOpened(vlmPojo.isOpened());
            luksVlmData.setDiskState(vlmPojo.getDiskState());
            luksVlmData.setUsableSize(vlmPojo.getUsableSize());
        }
    }

    private StorageRscData restoreStorageRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException
    {
        StorageRscPojo storRscPojo = (StorageRscPojo) rscDataPojo;
        StorageRscData storRscData = null;
        if (parent == null)
        {
            storRscData = rsc.getLayerData(apiCtx);
        }
        else
        {
            storRscData = findChild(parent, rscDataPojo.getId());
        }

        if (storRscData == null)
        {
            storRscData = layerDataFactory.createStorageRscData(
                storRscPojo.getId(),
                parent,
                rsc,
                storRscPojo.getRscNameSuffix()
            );
            if (parent == null)
            {
                rsc.setLayerData(apiCtx, storRscData);
            }
            else
            {
                updateChildsParent(storRscData, parent);
            }
        }
        else
        {
            storRscData.setParent(parent);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (VlmLayerDataApi vlmPojo : storRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                storRscData.remove(vlmNr);
            }
            else
            {
                restoreStorVlm(vlm, storRscData, vlmPojo);
            }
        }
        return storRscData;
    }

    private void restoreStorVlm(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo
    )
        throws AccessDeniedException, SQLException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        VlmProviderObject vlmData = storRscData.getVlmLayerObjects().get(vlmNr);
        switch (vlmPojo.getProviderKind())
        {
            case DISKLESS:
                if (vlmData == null || !(vlmData instanceof DisklessData))
                {
                    vlmData = layerDataFactory.createDisklessData(
                        vlm,
                        vlmPojo.getUsableSize(),
                        storRscData
                    );
                }
                else
                {
                    DisklessData drbdVlmData = (DisklessData) vlmData;
                    drbdVlmData.setUsableSize(vlmPojo.getUsableSize());
                }
                break;
            case LVM:
                if (vlmData == null || !(vlmData instanceof LvmData))
                {
                    vlmData = layerDataFactory.createLvmData(vlm, storRscData);
                }
                else
                {
                    LvmData lvmData = (LvmData) vlmData;
                    lvmData.setAllocatedSize(vlmPojo.getAllocatedSize());
                    lvmData.setDevicePath(vlmPojo.getDevicePath());
                    lvmData.setUsableSize(vlmPojo.getUsableSize());
                }
                break;
            case LVM_THIN:
                if (vlmData == null || !(vlmData instanceof LvmThinData))
                {
                    vlmData = layerDataFactory.createLvmThinData(vlm, storRscData);
                }
                else
                {
                    LvmThinData lvmThinData = (LvmThinData) vlmData;
                    lvmThinData.setAllocatedSize(vlmPojo.getAllocatedSize());
                    lvmThinData.setDevicePath(vlmPojo.getDevicePath());
                    lvmThinData.setUsableSize(vlmPojo.getUsableSize());
                }
                break;
            case SWORDFISH_INITIATOR:
                if (vlmData == null || !(vlmData instanceof SfInitiatorData))
                {
                    vlmData = layerDataFactory.createSfInitData(
                        vlm,
                        storRscData,
                        restoreSfVlmDfn(
                            vlm.getVolumeDefinition(),
                            ((SwordfishInitiatorVlmPojo) vlmPojo).getVlmDfn()
                        )
                    );
                }
                else
                {
                    SfInitiatorData sfInitData = (SfInitiatorData) vlmData;
                    sfInitData.setAllocatedSize(vlmPojo.getAllocatedSize());
                    sfInitData.setDevicePath(vlmPojo.getDevicePath());
                    sfInitData.setUsableSize(vlmPojo.getUsableSize());
                }
                break;
            case SWORDFISH_TARGET:
                if (vlmData == null || !(vlmData instanceof SfTargetData))
                {
                    vlmData = layerDataFactory.createSfTargetData(
                        vlm,
                        storRscData,
                        restoreSfVlmDfn(
                            vlm.getVolumeDefinition(),
                            ((SwordfishInitiatorVlmPojo) vlmPojo).getVlmDfn()
                        )
                    );
                }
                else
                {
                    SfTargetData sfTargetData = (SfTargetData) vlmData;
                    sfTargetData.setAllocatedSize(vlmPojo.getAllocatedSize());
                }
                break;
            case ZFS: // fall-through
            case ZFS_THIN:
                if (vlmData == null || !(vlmData instanceof ZfsData))
                {
                    vlmData = layerDataFactory.createZfsData(vlm, storRscData, vlmPojo.getProviderKind());
                }
                else
                {
                    ZfsData zfsData = (ZfsData) vlmData;
                    zfsData.setAllocatedSize(vlmPojo.getAllocatedSize());
                    zfsData.setDevicePath(vlmPojo.getDevicePath());
                    zfsData.setUsableSize(vlmPojo.getUsableSize());
                }
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected DeviceProviderKind: " + vlmPojo.getProviderKind());

        }
        storRscData.getVlmLayerObjects().put(vlmNr, vlmData);
    }

    private SfVlmDfnData restoreSfVlmDfn(
        VolumeDefinition vlmDfn,
        SwordfishVlmDfnPojo vlmDfnPojo
    )
        throws AccessDeniedException, SQLException
    {
        SfVlmDfnData sfVlmDfnData;
        VlmDfnLayerObject vlmDfnLayerData = vlmDfn.getLayerData(apiCtx, DeviceLayerKind.STORAGE);
        if (vlmDfnLayerData == null)
        {
            sfVlmDfnData = layerDataFactory.createSfVlmDfnData(
                vlmDfn,
                vlmDfnPojo.getVlmOdata(),
                vlmDfnPojo.getSuffixedRscName()
            );
            vlmDfn.setLayerData(apiCtx, sfVlmDfnData);
        }
        else
        {
            if (!(vlmDfnLayerData instanceof SfVlmDfnData))
            {
                throw new ImplementationError(
                    "Unexpected VolumeDefinition layer object. Swordfish expected, but got " +
                        vlmDfnLayerData.getClass().getSimpleName()
                );
            }
            sfVlmDfnData = (SfVlmDfnData) vlmDfnLayerData;
            sfVlmDfnData.setVlmOdata(vlmDfnPojo.getVlmOdata());
        }
        return sfVlmDfnData;
    }

    @SuppressWarnings("unchecked")
    private <T extends RscLayerObject> T findChild(RscLayerObject parent, int id)
    {
        RscLayerObject matchingChild = null;
        for (RscLayerObject child : parent.getChildren())
        {
            if (child.getRscLayerId() == id)
            {
                matchingChild = child;
                break;
            }
        }
        return (T) matchingChild;
    }

    private NvmeRscData restoreNvmeRscData(Resource rsc, RscLayerDataApi rscDataPojo, RscLayerObject parent)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException
    {
        NvmeRscPojo nvmeRscPojo = (NvmeRscPojo) rscDataPojo;

        NvmeRscData nvmeRscData = null;
        if (parent == null)
        {
            nvmeRscData = rsc.getLayerData(apiCtx);
        }
        else
        {
            nvmeRscData = findChild(parent, rscDataPojo.getId());
        }

        if (nvmeRscData == null)
        {
            nvmeRscData = layerDataFactory.createNvmeRscData(
                nvmeRscPojo.getId(),
                rsc,
                nvmeRscPojo.getRscNameSuffix(),
                parent
            );
            if (parent == null)
            {
                rsc.setLayerData(apiCtx, nvmeRscData);
            }
            else
            {
                updateChildsParent(nvmeRscData, parent);
            }
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (NvmeVlmPojo vlmPojo : nvmeRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                nvmeRscData.remove(vlmNr);
            }
            else
            {
                restoreNvmeVlm(vlm, nvmeRscData, vlmPojo);
            }
        }
        return nvmeRscData;
    }

    private void restoreNvmeVlm(Volume vlm, NvmeRscData nvmeRscData, NvmeVlmPojo vlmPojo)
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        NvmeVlmData nvmeVlmData = nvmeRscData.getVlmLayerObjects().get(vlmNr);
        if (nvmeVlmData == null)
        {
            nvmeVlmData = layerDataFactory.createNvmeVlmData(vlm, nvmeRscData);
            nvmeRscData.getVlmLayerObjects().put(vlmNr, nvmeVlmData);
        }
        else
        {
            nvmeVlmData.setAllocatedSize(vlmPojo.getAllocatedSize());
            nvmeVlmData.setDevicePath(vlmPojo.getDevicePath());
            nvmeVlmData.setDiskState(vlmPojo.getDiskState());
            nvmeVlmData.setUsableSize(vlmPojo.getUsableSize());
        }
    }

    private void updateChildsParent(RscLayerObject child, RscLayerObject newParent) throws SQLException
    {
        RscLayerObject oldParent = child.getParent();
        if (oldParent != null)
        {
            oldParent.getChildren().remove(child);
        }

        child.setParent(newParent);

        if (newParent != null)
        {
            newParent.getChildren().add(child);
        }
    }
}
