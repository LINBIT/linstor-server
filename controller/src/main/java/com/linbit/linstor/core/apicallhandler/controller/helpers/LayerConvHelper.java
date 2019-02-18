package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
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
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Singleton
@RemoveAfterDevMgrRework
public class LayerConvHelper
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final Props stltConf;
    private final LayerDataFactory layerDataFactory;
    private final DynamicNumberPool layerRscIdPool;

    @Inject
    public LayerConvHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        stltConf = stltConfRef;
        layerDataFactory = layerDataFactoryRef;
        layerRscIdPool = layerRscIdPoolRef;
    }

    /**
     * Ensures that the given {@link Resource} has the needed layer data.
     *
     * If the resource has only a swordfish volume, then only a storage layer is configured (swordfish).
     * Otherwise drbd, optionally crypt setup (if any volume definition is encrypted) and storage layer
     * are configured.
     * @throws AccessDeniedException
     */
    public void ensureDefaultLayerData(Resource rscRef)
    {
        try
        {
            boolean hasSwordfish = hasSwordfishKind(rscRef);
            if (hasSwordfish)
            {
                ensureStorageLayerCreated(rscRef, null);
            }
            else
            {
                // drbd + (crypt) + storage

                RscLayerObject rscObj = ensureDrbdRscLayerCreated(rscRef);
                if (needsCryptLayer(rscRef))
                {
                    rscObj = ensureCryptRscLayerCreated(rscRef, rscObj);
                }
                ensureStorageLayerCreated(rscRef, rscObj);
            }
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (ExhaustedPoolException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_RSC_LAYER_ID,
                    "Too many layered resources!"
                )
            );
        }
        catch (SQLException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "An sql excption occured while creating layer data"
                )
            );
        }
    }

    private boolean needsCryptLayer(Resource rscRef) throws AccessDeniedException
    {
        boolean needsCryptLayer = false;
        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();

            if (vlm.getVolumeDefinition().getFlags().isSet(apiCtx, VlmDfnFlags.ENCRYPTED))
            {
                needsCryptLayer = true;
                break;
            }
        }
        return needsCryptLayer;
    }

    private RscLayerObject ensureDrbdRscLayerCreated(
        Resource rscRef
    )
        throws AccessDeniedException, SQLException, InvalidKeyException, ExhaustedPoolException
    {
        ResourceDefinition rscDfn = rscRef.getDefinition();
        DrbdRscDfnData drbdRscDfnData = rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
        if (drbdRscDfnData == null)
        {
            drbdRscDfnData = layerDataFactory.createDrbdRscDfnData(
                rscDfn,
                "",
                getAndCheckPeerSlotsForNewResource((ResourceDefinitionData) rscDfn),
                ConfigModule.DEFAULT_AL_STRIPES,
                ConfigModule.DEFAULT_AL_SIZE,
                // TODO: remove rscDfn.getPort and allocate and store that information only in drbdRscDfnData
                rscDfn.getPort(apiCtx),
                // TODO: remove rscDfn.getTransportType and store that information only in drbdRscDfnData
                rscDfn.getTransportType(apiCtx),
                // TODO: remove rscDfn.getSecret and store that information only in drbdRscDfnData
                rscDfn.getSecret(apiCtx)
            );
            rscDfn.setLayerData(apiCtx, drbdRscDfnData);
        }

        DrbdRscData drbdRscData = rscRef.getLayerData(apiCtx);
        if (drbdRscData == null)
        {
            drbdRscData = layerDataFactory.createDrbdRscData(
                layerRscIdPool.autoAllocate(),
                rscRef,
                "",
                null, // no parent
                drbdRscDfnData,
                // TODO: remove rsc.getNodeId() and allocate and store that information only in drbdRscData
                rscRef.getNodeId(),
                null,
                null,
                null,
                rscRef.getStateFlags().getFlagsBits(apiCtx)
            );
            rscRef.setLayerData(apiCtx, drbdRscData);
            drbdRscDfnData.getDrbdRscDataList().add(drbdRscData);
        }
        else
        {
            drbdRscData.getFlags().resetFlagsTo(
                apiCtx,
                DrbdRscFlags.restoreFlags(rscRef.getStateFlags().getFlagsBits(apiCtx))
            );
        }

        Map<VolumeNumber, DrbdVlmData> drbdVlmDataMap = drbdRscData.getVlmLayerObjects();
        List<VolumeNumber> vlmsExistingButShouldNot = new ArrayList<>(
            drbdVlmDataMap.keySet()
        );

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            vlmsExistingButShouldNot.remove(vlmDfn.getVolumeNumber());

            if (!drbdVlmDataMap.containsKey(vlmDfn.getVolumeNumber()))
            {
                DrbdVlmDfnData drbdVlmDfnData = vlmDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
                if (drbdVlmDfnData == null)
                {
                    drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                        vlmDfn,
                        "",
                        // TODO: remove vlmDfn.getMinorNr and allocate and store that information only in drbdVlmDfnData
                        vlmDfn.getMinorNr(apiCtx)
                    );
                    vlmDfn.setLayerData(apiCtx, drbdVlmDfnData);
                }

                DrbdVlmData drbdVlmData = layerDataFactory.createDrbdVlmData(vlm, drbdRscData, drbdVlmDfnData);
                drbdVlmDataMap.put(vlmDfn.getVolumeNumber(), drbdVlmData);
            }
        }

        for (VolumeNumber vlmNr : vlmsExistingButShouldNot)
        {
            drbdVlmDataMap.remove(vlmNr);
        }

        return drbdRscData;
    }

    private RscLayerObject ensureCryptRscLayerCreated(
        Resource rscRef,
        RscLayerObject parentRscData
    )
        throws ExhaustedPoolException, AccessDeniedException, SQLException
    {
        CryptSetupRscData cryptRscData;
        if (parentRscData.getChildren().isEmpty())
        {
            cryptRscData = layerDataFactory.createCryptSetupRscData(
                layerRscIdPool.autoAllocate(),
                rscRef,
                "",
                parentRscData
            );
            parentRscData.getChildren().add(cryptRscData);
        }
        else
        {
            cryptRscData = (CryptSetupRscData) parentRscData.getChildren().iterator().next();
        }

        Map<VolumeNumber, CryptSetupVlmData> vlmLayerObjects = cryptRscData.getVlmLayerObjects();
        List<VolumeNumber> existingVlmsDataToBeDeleted = new ArrayList<>(vlmLayerObjects.keySet());

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();

            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
            if (!vlmLayerObjects.containsKey(vlmNr))
            {
                CryptSetupVlmData cryptVlmData = layerDataFactory.createCryptSetupVlmData(
                    vlm,
                    cryptRscData,
                    // TODO: remove vlmDfn.getCryptKey and allocate and store that information only in cryptVlmData
                    vlmDfn.getCryptKey(apiCtx).getBytes()
                );
                vlmLayerObjects.put(vlmNr, cryptVlmData);
            }
            existingVlmsDataToBeDeleted.remove(vlmNr);
        }

        for (VolumeNumber vlmNr : existingVlmsDataToBeDeleted)
        {
            vlmLayerObjects.remove(vlmNr);
        }

        return cryptRscData;
    }

    private boolean hasSwordfishKind(Resource rscRef) throws AccessDeniedException
    {
        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        boolean foundSwordfishKind = false;
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            DeviceProviderKind kind = vlm.getStorPool(apiCtx).getDeviceProviderKind();
            if (kind.equals(DeviceProviderKind.SWORDFISH_INITIATOR) ||
                    kind.equals(DeviceProviderKind.SWORDFISH_TARGET))
            {
                foundSwordfishKind = true;
                break;
            }
        }
        return foundSwordfishKind;
    }

    private void ensureStorageLayerCreated(
        Resource rscRef,
        RscLayerObject parentRscData
    )
        throws SQLException, AccessDeniedException, ExhaustedPoolException
    {
        StorageRscData rscData = null;
        if (parentRscData == null)
        {
            rscData = rscRef.getLayerData(apiCtx);
        }
        else
        {
            if (!parentRscData.getChildren().isEmpty())
            {
                rscData = (StorageRscData) parentRscData.getChildren().iterator().next();
            }
        }

        if (rscData == null)
        {
            rscData = layerDataFactory.createStorageRscData(
                layerRscIdPool.autoAllocate(),
                parentRscData,
                rscRef,
                ""
            );
            if (parentRscData == null)
            {
                rscRef.setLayerData(apiCtx, rscData);
            }
            else
            {
                parentRscData.getChildren().add(rscData);
            }
        }

        Map<VolumeNumber, VlmProviderObject> vlmDataMap = rscData.getVlmLayerObjects();
        List<VolumeNumber> existingVlmsDataToBeDeleted = new ArrayList<>(vlmDataMap.keySet());

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            VlmDfnLayerObject vlmDfnData = vlm.getVolumeDefinition().getLayerData(
                apiCtx,
                DeviceLayerKind.STORAGE
            );

            DeviceProviderKind kind = vlm.getStorPool(apiCtx).getDeviceProviderKind();
            VlmProviderObject vlmData = rscData.getVlmProviderObject(vlm.getVolumeDefinition().getVolumeNumber());
            if (vlmData == null)
            {
                switch (kind)
                {
                    case SWORDFISH_INITIATOR:
                        {
                            if (vlmDfnData == null)
                            {
                                vlmDfnData = layerDataFactory.createSfVlmDfnData(vlm.getVolumeDefinition(), null, "");
                                vlm.getVolumeDefinition().setLayerData(apiCtx, vlmDfnData);
                            }
                            if (!(vlmDfnData instanceof SfVlmDfnData))
                            {
                                throw new ImplementationError(
                                    "Unexpected type of volume definition storage data: " +
                                        vlmDfnData.getClass().getSimpleName()
                                );
                            }

                            vlmData = layerDataFactory.createSfInitData(
                                vlm,
                                rscData,
                                (SfVlmDfnData) vlmDfnData
                            );
                        }
                        break;
                    case SWORDFISH_TARGET:
                        {
                            if (vlmDfnData == null)
                            {
                                vlmDfnData = layerDataFactory.createSfVlmDfnData(vlm.getVolumeDefinition(), null, "");
                                vlm.getVolumeDefinition().setLayerData(apiCtx, vlmDfnData);
                            }
                            if (!(vlmDfnData instanceof SfVlmDfnData))
                            {
                                throw new ImplementationError(
                                    "Unexpected type of volume definition storage data: " +
                                        vlmDfnData.getClass().getSimpleName()
                                );
                            }
                            vlmData = layerDataFactory.createSfTargetData(
                                vlm,
                                rscData,
                                (SfVlmDfnData) vlmDfnData
                            );
                        }
                        break;
                    case DRBD_DISKLESS:
                        vlmData = layerDataFactory.createDrbdDisklessData(
                            vlm,
                            vlm.getVolumeDefinition().getVolumeSize(apiCtx),
                            rscData
                        );
                        break;
                    case LVM:
                        vlmData = layerDataFactory.createLvmData(vlm, rscData);
                        break;
                    case LVM_THIN:
                        vlmData = layerDataFactory.createLvmThinData(vlm, rscData);
                        break;
                    case ZFS: // fall-through
                    case ZFS_THIN:
                        vlmData = layerDataFactory.createZfsData(vlm, rscData, kind);
                        break;
                    case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
                    default:
                        throw new ImplementationError("Unexpected kind: " + kind);
                }
            }

            VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
            existingVlmsDataToBeDeleted.remove(vlmNr);
            vlmDataMap.put(
                vlmNr,
                vlmData
            );
        }

        for (VolumeNumber vlmNr : existingVlmsDataToBeDeleted)
        {
            vlmDataMap.remove(vlmNr);
        }
    }

    private short getAndCheckPeerSlotsForNewResource(ResourceDefinitionData rscDfn)
        throws InvalidKeyException, AccessDeniedException
    {
        int resourceCount = rscDfn.getResourceCount();

        String peerSlotsNewResourceProp = new PriorityProps(rscDfn.getProps(apiCtx), stltConf)
            .getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE);
        short peerSlots = peerSlotsNewResourceProp == null ?
            InternalApiConsts.DEFAULT_PEER_SLOTS :
            Short.valueOf(peerSlotsNewResourceProp);

        if (peerSlots < resourceCount)
        {
            String detailsMsg = (peerSlotsNewResourceProp == null ? "Default" : "Configured") +
                " peer slot count " + peerSlots + " too low";
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS, "Insufficient peer slots to create resource")
                .setDetails(detailsMsg)
                .setCorrection("Configure a higher peer slot count on the resource definition or controller")
                .build()
            );
        }
        return peerSlots;
    }
}
