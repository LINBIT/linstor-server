package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
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
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Singleton
public class CtrlLayerStackHelper
{
    private static final int SECRET_KEY_BYTES = 20;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final Props stltConf;
    private final LayerDataFactory layerDataFactory;
    private final DynamicNumberPool layerRscIdPool;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final CtrlSecurityObjects secObjs;

    @Inject
    public CtrlLayerStackHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        CtrlSecurityObjects secObjsRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        stltConf = stltConfRef;
        layerDataFactory = layerDataFactoryRef;
        layerRscIdPool = layerRscIdPoolRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        secObjs = secObjsRef;
    }

    public List<DeviceLayerKind> getLayerStack(Resource rscRef)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        try
        {
            RscLayerObject layerData = rscRef.getLayerData(apiCtx);
            while (layerData != null)
            {
                ret.add(layerData.getLayerKind());
                layerData = layerData.getFirstChild();
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    /**
     * Creates the linstor default stack, which is {@link DeviceLayerKind#DRBD} on an optional
     * {@link DeviceLayerKind#LUKS} on a {@link DeviceLayerKind#STORAGE} layer.
     * A LUKS layer is created if at least one {@link VolumeDefinition}
     * has the {@link VolumeDefinition.VlmDfnFlags#ENCRYPTED} flag set.
     * @param accCtxRef
     * @return
     */
    public List<DeviceLayerKind> createDefaultStack(AccessContext accCtxRef, Resource rscRef)
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            boolean hasSwordfish = hasSwordfishKind(accCtxRef, rscRef);
            if (hasSwordfish)
            {
                layerStack = Arrays.asList(DeviceLayerKind.STORAGE);
            }
            else
            {
                // drbd + (luks) + storage
                if (needsLuksLayer(accCtxRef, rscRef))
                {
                    layerStack = Arrays.asList(
                        DeviceLayerKind.DRBD,
                        DeviceLayerKind.LUKS,
                        DeviceLayerKind.STORAGE
                    );
                }
                else
                {
                    layerStack = Arrays.asList(
                        DeviceLayerKind.DRBD,
                        DeviceLayerKind.STORAGE
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return layerStack;
    }

    public void ensureRscDfnLayerDataExitsIfNeeded(
        ResourceDefinitionData rscDfn,
        Integer tcpPortNrIntRef,
        TransportType transportTypeRef,
        String secretRef,
        Short newRscPeerSlotsRef
    )
        throws SQLException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            layerStack = rscDfn.getLayerStack(apiCtx);
            if (layerStack.contains(DeviceLayerKind.DRBD) ||
                tcpPortNrIntRef != null ||
                transportTypeRef != null ||
                secretRef != null
            )
            {
                ensureDrbdRscDfnExists(
                    rscDfn,
                    tcpPortNrIntRef,
                    transportTypeRef,
                    secretRef,
                    newRscPeerSlotsRef
                );
            }
            else
            {
                DrbdRscDfnData drbdRscDfnData = rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
                if (drbdRscDfnData != null)
                {
                    rscDfn.removeLayerData(apiCtx, DeviceLayerKind.DRBD);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public DrbdRscDfnData ensureDrbdRscDfnExists(
        ResourceDefinitionData rscDfn,
        Integer tcpPortNrIntRef,
        TransportType transportTypeRef,
        String secretRef,
        Short newRscPeerSlotsRef
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException,
        ExhaustedPoolException, ValueInUseException
    {
        DrbdRscDfnData drbdRscDfnData = rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
        if (drbdRscDfnData == null)
        {
            TransportType transportType = transportTypeRef;
            String secret = secretRef;
            Short peerSlots = newRscPeerSlotsRef;
            if (secret == null)
            {
                secret = SecretGenerator.generateSecretString(SecretGenerator.DRBD_SHARED_SECRET_SIZE);
            }
            if (transportType == null)
            {
                transportType = TransportType.IP;
            }
            if (peerSlots == null)
            {
                peerSlots = getAndCheckPeerSlotsForNewResource(rscDfn);
            }

            drbdRscDfnData = layerDataFactory.createDrbdRscDfnData(
                rscDfn,
                "",
                peerSlots,
                InternalApiConsts.DEFAULT_AL_STRIPES,
                InternalApiConsts.DEFAULT_AL_SIZE,
                tcpPortNrIntRef,
                transportType,
                secret
            );
            rscDfn.setLayerData(apiCtx, drbdRscDfnData);
        }
        else
        {
            if (tcpPortNrIntRef != null)
            {
                drbdRscDfnData.setPort(tcpPortNrIntRef);
            }
            if (transportTypeRef != null)
            {
                drbdRscDfnData.setTransportType(transportTypeRef);
            }
            if (secretRef != null)
            {
                drbdRscDfnData.setSecret(secretRef);
            }
            if (newRscPeerSlotsRef != null)
            {
                drbdRscDfnData.setPeerSlots(newRscPeerSlotsRef);
            }
        }
        return drbdRscDfnData;
    }

    public void ensureVlmDfnLayerDataExits(
        VolumeDefinitionData vlmDfn,
        Integer minorNrInt
    )
        throws SQLException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException
    {
        try
        {
            List<DeviceLayerKind> layerStack = vlmDfn.getResourceDefinition().getLayerStack(apiCtx);
            if (layerStack.contains(DeviceLayerKind.DRBD) || minorNrInt != null)
            {
                DrbdRscDfnData drbdRscDfnData = ensureDrbdRscDfnExists(
                    (ResourceDefinitionData) vlmDfn.getResourceDefinition(),
                    null,
                    null,
                    null,
                    null
                );
                DrbdVlmDfnData drbdVlmDfn = drbdRscDfnData.getDrbdVlmDfn(vlmDfn.getVolumeNumber());
                if (drbdVlmDfn == null)
                {
                    vlmDfn.setLayerData(
                        apiCtx,
                        layerDataFactory.createDrbdVlmDfnData(
                            vlmDfn,
                            "",
                            minorNrInt,
                            drbdRscDfnData
                        )
                    );
                }
                else
                {
                    // minor numer is not changable
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void ensureStackDataExists(
        ResourceData rscDataRef,
        List<DeviceLayerKind> layerStackRef,
        Integer nodeIdIntRef
    )
    {
        try
        {
            List<DeviceLayerKind> layerStack;
            if (layerStackRef == null)
            {
                layerStack = getLayerStack(rscDataRef);
            }
            else
            {
                layerStack = layerStackRef;
            }

            RscLayerObject rscObj = null;
            for (DeviceLayerKind kind : layerStack)
            {
                switch (kind)
                {
                    case DRBD:
                        rscObj = ensureDrbdRscLayerCreated(rscDataRef, nodeIdIntRef);
                        break;
                    case LUKS:
                        rscObj = ensureLuksRscLayerCreated(rscDataRef, rscObj);
                        break;
                    case STORAGE:
                        ensureStorageLayerCreated(rscDataRef, rscObj);
                        break;
                    case NVME:
                        rscObj = ensureNvmeRscLayerCreated(rscDataRef, rscObj);
                        break;
                    default:
                        break;
                }
            }
        }
        catch (AccessDeniedException exc)
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
                ),
                exc
            );
        }
        catch (SQLException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "An sql excption occured while creating layer data"
                ),
                exc
            );
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An excption occured while creating layer data"
                ),
                exc
            );
        }
    }

    private boolean needsLuksLayer(AccessContext accCtxRef, Resource rscRef)
        throws AccessDeniedException
    {
        boolean needsLuksLayer = false;
        Iterator<VolumeDefinition> iterateVolumeDefinitions = rscRef.getDefinition().iterateVolumeDfn(apiCtx);
        while (iterateVolumeDefinitions.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDefinitions.next();

            if (vlmDfn.getFlags().isSet(apiCtx, VlmDfnFlags.ENCRYPTED))
            {
                needsLuksLayer = true;
                break;
            }
        }
        return needsLuksLayer;
    }

    private boolean hasSwordfishKind(AccessContext accCtxRef, Resource rscRef)
        throws AccessDeniedException
    {
        boolean foundSwordfishKind = false;

        Iterator<VolumeDefinition> iterateVolumeDfn = rscRef.getDefinition().iterateVolumeDfn(apiCtx);
        while (iterateVolumeDfn.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDfn.next();
            StorPool storPool = storPoolResolveHelper.resolveStorPool(
                accCtxRef,
                rscRef,
                vlmDfn,
                rscRef.getStateFlags().isSet(apiCtx, RscFlags.DISKLESS),
                false
            ).getValue();

            if (storPool != null)
            {
                DeviceProviderKind kind = storPool.getDeviceProviderKind();
                if (kind.equals(DeviceProviderKind.SWORDFISH_INITIATOR) ||
                    kind.equals(DeviceProviderKind.SWORDFISH_TARGET))
                {
                    foundSwordfishKind = true;
                    break;
                }
            }
        }
        return foundSwordfishKind;
    }

    private RscLayerObject ensureDrbdRscLayerCreated(
        Resource rscRef,
        Integer nodeIdIntRef
    )
        throws AccessDeniedException, SQLException, ExhaustedPoolException, ValueOutOfRangeException,
            ValueInUseException
    {
        ResourceDefinition rscDfn = rscRef.getDefinition();
        DrbdRscDfnData drbdRscDfnData = rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
        if (drbdRscDfnData == null)
        {
            drbdRscDfnData = layerDataFactory.createDrbdRscDfnData(
                rscDfn,
                "",
                getAndCheckPeerSlotsForNewResource((ResourceDefinitionData) rscDfn),
                InternalApiConsts.DEFAULT_AL_STRIPES,
                InternalApiConsts.DEFAULT_AL_SIZE,
                null, // generated tcpPort
                TransportType.IP,
                SecretGenerator.generateSharedSecret()
            );
            rscDfn.setLayerData(apiCtx, drbdRscDfnData);
        }

        DrbdRscData drbdRscData = rscRef.getLayerData(apiCtx);
        if (drbdRscData == null)
        {
            NodeId nodeId;
            if (nodeIdIntRef == null)
            {
                try
                {
                    int[] occupiedIds = new int[drbdRscDfnData.getDrbdRscDataList().size()];
                    int idx = 0;
                    for (DrbdRscData tmpDrbdRscData : drbdRscDfnData.getDrbdRscDataList())
                    {
                        occupiedIds[idx] = tmpDrbdRscData.getNodeId().value;
                        ++idx;
                    }
                    Arrays.sort(occupiedIds);

                    nodeId = NodeIdAlloc.getFreeNodeId(occupiedIds);
                }
                catch (ExhaustedPoolException exhaustedPoolExc)
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_POOL_EXHAUSTED_NODE_ID,
                        "An exception occured during generation of a node ID."
                    ), exhaustedPoolExc);
                }
            }
            else
            {
                nodeId = new NodeId(nodeIdIntRef);
            }

            drbdRscData = layerDataFactory.createDrbdRscData(
                layerRscIdPool.autoAllocate(),
                rscRef,
                "",
                null, // no parent
                drbdRscDfnData,
                nodeId,
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
                        null, // generated minor
                        drbdRscDfnData
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

    private RscLayerObject ensureLuksRscLayerCreated(
        Resource rscRef,
        RscLayerObject parentRscData
    )
        throws ExhaustedPoolException, SQLException, LinStorException
    {
        LuksRscData luksRscData = null;
        if (parentRscData == null)
        {
            luksRscData = rscRef.getLayerData(apiCtx);
        }
        else
        {
            if (!parentRscData.getChildren().isEmpty())
            {
                luksRscData = (LuksRscData) parentRscData.getChildren().iterator().next();
            }
        }
        if (luksRscData == null)
        {
            luksRscData = layerDataFactory.createLuksRscData(
                layerRscIdPool.autoAllocate(),
                rscRef,
                "",
                parentRscData
            );
            if (parentRscData == null)
            {
                rscRef.setLayerData(apiCtx, luksRscData);
            }
            else
            {
                parentRscData.getChildren().add(luksRscData);
            }
        }

        Map<VolumeNumber, LuksVlmData> vlmLayerObjects = luksRscData.getVlmLayerObjects();
        List<VolumeNumber> existingVlmsDataToBeDeleted = new ArrayList<>(vlmLayerObjects.keySet());

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();

            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
            if (!vlmLayerObjects.containsKey(vlmNr))
            {
                byte[] masterKey = secObjs.getCryptKey();
                if (masterKey == null || masterKey.length == 0)
                {
                    throw new ApiRcException(ApiCallRcImpl
                        .entryBuilder(ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                            "Unable to create an encrypted volume definition without having a master key")
                        .setCause("The masterkey was not initialized yet")
                        .setCorrection("Create or enter the master passphrase")
                        .build()
                    );
                }

                String vlmDfnKeyPlain = SecretGenerator.generateSecretString(SECRET_KEY_BYTES);
                SymmetricKeyCipher cipher;
                cipher = SymmetricKeyCipher.getInstanceWithKey(masterKey);

                byte[] encryptedVlmDfnKey = cipher.encrypt(vlmDfnKeyPlain.getBytes());

                LuksVlmData luksVlmData = layerDataFactory.createLuksVlmData(
                    vlm,
                    luksRscData,
                    encryptedVlmDfnKey
                );
                vlmLayerObjects.put(vlmNr, luksVlmData);
            }
            existingVlmsDataToBeDeleted.remove(vlmNr);
        }

        for (VolumeNumber vlmNr : existingVlmsDataToBeDeleted)
        {
            vlmLayerObjects.remove(vlmNr);
        }

        return luksRscData;
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
                    case DISKLESS:
                        vlmData = layerDataFactory.createDisklessData(
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

    private RscLayerObject ensureNvmeRscLayerCreated(
        Resource rscRef,
        RscLayerObject parentRscData
    )
        throws ExhaustedPoolException, SQLException, LinStorException
    {
        NvmeRscData nvmeRscData = null;
        if (parentRscData == null)
        {
            nvmeRscData = rscRef.getLayerData(apiCtx);
        }
        else
        {
            if (!parentRscData.getChildren().isEmpty())
            {
                nvmeRscData = (NvmeRscData) parentRscData.getChildren().iterator().next();
            }
        }
        if (nvmeRscData == null)
        {
            nvmeRscData = layerDataFactory.createNvmeRscData(
                layerRscIdPool.autoAllocate(),
                rscRef,
                "",
                parentRscData
            );
            if (parentRscData == null)
            {
                rscRef.setLayerData(apiCtx, nvmeRscData);
            }
            else
            {
                parentRscData.getChildren().add(nvmeRscData);
            }
        }

        Map<VolumeNumber, NvmeVlmData> vlmLayerObjects = nvmeRscData.getVlmLayerObjects();
        List<VolumeNumber> existingVlmsDataToBeDeleted = new ArrayList<>(vlmLayerObjects.keySet());

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();

            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
            if (!vlmLayerObjects.containsKey(vlmNr))
            {
                NvmeVlmData nvmeVlmData = layerDataFactory.createNvmeVlmData(vlm, nvmeRscData);
                vlmLayerObjects.put(vlmNr, nvmeVlmData);
            }
            existingVlmsDataToBeDeleted.remove(vlmNr);
        }

        for (VolumeNumber vlmNr : existingVlmsDataToBeDeleted)
        {
            vlmLayerObjects.remove(vlmNr);
        }

        return nvmeRscData;
    }

    private short getAndCheckPeerSlotsForNewResource(ResourceDefinitionData rscDfn)
        throws AccessDeniedException
    {
        short peerSlots;

        try
        {
            int resourceCount = rscDfn.getResourceCount();
            String peerSlotsNewResourceProp = new PriorityProps(rscDfn.getProps(apiCtx), stltConf)
                .getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE);
            peerSlots = peerSlotsNewResourceProp == null ?
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
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return peerSlots;
    }
}
