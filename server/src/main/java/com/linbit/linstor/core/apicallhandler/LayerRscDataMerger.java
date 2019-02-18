package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo.CryptVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishInitiatorVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupRscData;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
            RscLayerDataPojo rscLayerDataPojo,
            RscLayerObject parent
        )
            throws AccessDeniedException, SQLException, ValueOutOfRangeException;
    }

    public void restoreLayerData(
        Resource rsc,
        RscLayerDataPojo rscLayerDataPojo
    )
    {
        try
        {
            restore(rsc, rscLayerDataPojo, null);
        }
        catch (AccessDeniedException | SQLException | ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void restore(
        Resource rsc,
        RscLayerDataPojo rscLayerDataPojo,
        RscLayerObject parent
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException
    {
        RscDataExtractor extractor;
        switch (rscLayerDataPojo.getLayerKind())
        {
            case DRBD:
                extractor = this::restoreDrbdRscData;
                break;
            case CRYPT_SETUP:
                extractor = this::restoreCryptSetupRscData;
                break;
            case STORAGE:
                extractor = this::restoreStorageRscData;
                break;
            default:
                throw new ImplementationError("Unexpected layer kind: " + rscLayerDataPojo.getLayerKind());
        }
        RscLayerObject rscLayerObject = extractor.restore(rsc, rscLayerDataPojo, parent);
        for (RscLayerDataPojo childRscPojo : rscLayerDataPojo.getChildren())
        {
            restore(rsc, childRscPojo, rscLayerObject);
        }
    }

    private DrbdRscData restoreDrbdRscData(
        Resource rsc,
        RscLayerDataPojo rscDataPojo,
        RscLayerObject parent
    )
        throws SQLException, ValueOutOfRangeException, AccessDeniedException
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

        Set<VolumeNumber> vlmNrsToDelete = new HashSet<>(drbdRscData.getVlmLayerObjects().keySet());

        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            restoreDrbdVlm(vlm, drbdRscData, drbdRscPojo.getVolumeList());
            vlmNrsToDelete.remove(vlm.getVolumeDefinition().getVolumeNumber());
        }

        for (VolumeNumber vlmNrToDelete : vlmNrsToDelete)
        {
            drbdRscData.remove(vlmNrToDelete);
        }

        return drbdRscData;
    }

    private DrbdRscDfnData restoreDrbdRscDfn(
        ResourceDefinition rscDfn,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, SQLException, ValueOutOfRangeException, AccessDeniedException
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
                new TcpPortNumber(drbdRscDfnPojo.getPort()),
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

    private void restoreDrbdVlm(Volume vlm, DrbdRscData rscData, List<DrbdVlmPojo> vlmPojos)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
        DrbdVlmPojo vlmPojo = null;
        {
            for (DrbdVlmPojo drbdVlmPojo : vlmPojos)
            {
                if (drbdVlmPojo.getDrbdVlmDfn().getVlmNr() == vlmNr.value)
                {
                    vlmPojo = drbdVlmPojo;
                    break;
                }
            }
            if (vlmPojo == null)
            {
                throw new ImplementationError("No DrbdVlmPojo found for " + vlm);
            }
        }

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
        // nothing to restore
    }

    private DrbdVlmDfnData restoreDrbdVlmDfn(VolumeDefinition vlmDfn, DrbdVlmDfnPojo drbdVlmDfnPojo)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException
    {
        DrbdVlmDfnData drbdVlmDfnData = vlmDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
        if (drbdVlmDfnData == null)
        {
            drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                vlmDfn,
                drbdVlmDfnPojo.getRscNameSuffix(),
                new MinorNumber(drbdVlmDfnPojo.getMinorNr())
            );
            vlmDfn.setLayerData(apiCtx, drbdVlmDfnData);
        }
        // nothing to merge
        return drbdVlmDfnData;
    }

    private CryptSetupRscData restoreCryptSetupRscData(
        Resource rsc,
        RscLayerDataPojo rscDataPojo,
        RscLayerObject parent
    )
        throws AccessDeniedException, SQLException
    {
        CryptSetupRscPojo cryptRscPojo = (CryptSetupRscPojo) rscDataPojo;

        CryptSetupRscData cryptRscData = null;
        if (parent == null)
        {
            cryptRscData = rsc.getLayerData(apiCtx);
        }
        else
        {
            cryptRscData = findChild(parent, rscDataPojo.getId());
        }

        if (cryptRscData == null)
        {
            cryptRscData = layerDataFactory.createCryptSetupRscData(
                cryptRscPojo.getId(),
                rsc,
                cryptRscPojo.getRscNameSuffix(),
                parent
            );
            if (parent == null)
            {
                rsc.setLayerData(apiCtx, cryptRscData);
            }
            else
            {
                updateChildsParent(cryptRscData, parent);
            }
        }

        Set<VolumeNumber> vlmNrsToDelete = new HashSet<>(cryptRscData.getVlmLayerObjects().keySet());

        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            restoreCryptVlm(vlm, cryptRscData, cryptRscPojo.getVolumeList());

            vlmNrsToDelete.remove(vlm.getVolumeDefinition().getVolumeNumber());
        }

        for (VolumeNumber vlmNrToDelete : vlmNrsToDelete)
        {
            cryptRscData.remove(vlmNrToDelete);
        }
        return cryptRscData;
    }

    private void restoreCryptVlm(
        Volume vlm,
        CryptSetupRscData cryptRscData,
        List<CryptVlmPojo> vlmPojos
    )
        throws SQLException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
        CryptVlmPojo vlmPojo = null;
        {
            for (CryptVlmPojo cryptVlmPojo : vlmPojos)
            {
                if (cryptVlmPojo.getVlmNr() == vlmNr.value)
                {
                    vlmPojo = cryptVlmPojo;
                    break;
                }
            }
            if (vlmPojo == null)
            {
                throw new ImplementationError("No CryptVlmPojo found for " + vlm);
            }
        }

        CryptSetupVlmData cryptVlmData = cryptRscData.getVlmLayerObjects().get(vlmNr);
        if (cryptVlmData == null)
        {
            cryptVlmData = layerDataFactory.createCryptSetupVlmData(
                vlm,
                cryptRscData,
                vlmPojo.getEncryptedPassword()
            );
            cryptRscData.getVlmLayerObjects().put(vlmNr, cryptVlmData);
        }
        // nothing to restore
    }

    private StorageRscData restoreStorageRscData(
        Resource rsc,
        RscLayerDataPojo rscDataPojo,
        RscLayerObject parent
    )
        throws AccessDeniedException, SQLException
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

        Set<VolumeNumber> vlmNrsToDelete = new HashSet<>(storRscData.getVlmLayerObjects().keySet());

        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            restoreStorVlm(vlm, storRscData, storRscPojo.getVolumeList());

            vlmNrsToDelete.remove(vlm.getVolumeDefinition().getVolumeNumber());
        }

        for (VolumeNumber vlmNrToDelete : vlmNrsToDelete)
        {
            storRscData.remove(vlmNrToDelete);
        }

        return storRscData;
    }

    private void restoreStorVlm(
        Volume vlm,
        StorageRscData storRscData,
        List<VlmLayerDataPojo> vlmPojos
    )
        throws AccessDeniedException, SQLException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
        VlmLayerDataPojo vlmPojo = null;
        {
            for (VlmLayerDataPojo vlmLayerDataPojo : vlmPojos)
            {
                if (vlmLayerDataPojo.getVlmNr() == vlmNr.value)
                {
                    vlmPojo = vlmLayerDataPojo;
                    break;
                }
            }
            if (vlmPojo == null)
            {
                throw new ImplementationError("No VlmLayerDataPojo found for " + vlm);
            }
        }

        VlmProviderObject vlmData = storRscData.getVlmLayerObjects().get(vlmNr);
        if (vlmData == null)
        {
            switch (vlmPojo.getProviderKind())
            {
                case DRBD_DISKLESS:
                    vlmData = layerDataFactory.createDrbdDisklessData(
                        vlm,
                        vlmPojo.getUsableSize(),
                        storRscData
                    );
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
                        restoreSfVlmDfn(
                            vlm.getVolumeDefinition(),
                            ((SwordfishInitiatorVlmPojo) vlmPojo).getVlmDfn()
                        )
                    );
                    break;
                case SWORDFISH_TARGET:
                    vlmData = layerDataFactory.createSfTargetData(
                        vlm,
                        storRscData,
                        restoreSfVlmDfn(
                            vlm.getVolumeDefinition(),
                            ((SwordfishInitiatorVlmPojo) vlmPojo).getVlmDfn()
                        )
                    );
                    break;
                case ZFS: // fall-through
                case ZFS_THIN:
                    vlmData = layerDataFactory.createZfsData(vlm, storRscData, vlmPojo.getProviderKind());
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                default:
                    throw new ImplementationError("Unexpected DeviceProviderKind: " + vlmPojo.getProviderKind());

            }
            storRscData.getVlmLayerObjects().put(vlmNr, vlmData);
        }
        // nothing to restore
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
