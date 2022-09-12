package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SecretGenerator;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
class RscLuksLayerHelper extends AbsRscLayerHelper<
    LuksRscData<Resource>, LuksVlmData<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    private static final int SECRET_KEY_BYTES = 20;

    private final CtrlSecurityObjects secObjs;
    private final Provider<RscNvmeLayerHelper> nvmeHelperProvider;
    private final SharedResourceManager sharedRscMgr;

    ModularCryptoProvider cryptoProvider;
    private final EncryptionHelper encryptionHelper;

    @Inject
    RscLuksLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        CtrlSecurityObjects secObjsRef,
        ModularCryptoProvider cryptoProviderRef,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory,
        Provider<RscNvmeLayerHelper> nvmeHelperProviderRef,
        SharedResourceManager sharedRscMgrRef,
        EncryptionHelper encryptionHelperRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            // LuksRscData.class cannot directly be casted to Class<LuksRscData<Resource>>. because java.
            // its type is Class<LuksRscData> (without nested types), but that is not enough as the super constructor
            // wants a Class<RSC_PO>, where RSC_PO is LuksRscData<Resource>.
            (Class<LuksRscData<Resource>>) ((Object) LuksRscData.class),
            DeviceLayerKind.LUKS,
            rscLayerDataFactory
        );
        secObjs = secObjsRef;
        cryptoProvider = cryptoProviderRef;
        nvmeHelperProvider = nvmeHelperProviderRef;
        sharedRscMgr = sharedRscMgrRef;
        encryptionHelper = encryptionHelperRef;
    }

    @Override
    protected RscDfnLayerObject createRscDfnData(
        ResourceDefinition rscDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // LuksLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Luks specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // LuksLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Luks specific volume-definition, nothing to merge
    }

    @Override
    protected LuksRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createLuksRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    @Override
    protected void mergeRscData(LuksRscData<Resource> rscDataRef, LayerPayload payloadRef)
    {
        // nothing to merge
    }

    @Override
    protected boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return true;
    }

    @Override
    protected Set<StorPool> getNeededStoragePools(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        // no special storage pools needed
        return Collections.emptySet();
    }

    @Override
    protected LuksVlmData<Resource> createVlmLayerData(
        LuksRscData<Resource> luksRscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException, InvalidNameException
    {
        byte[] encryptedVlmKey = null;

        Resource rsc = vlm.getAbsResource();
        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isOpenflexBelow = layerListRef.contains(DeviceLayerKind.OPENFLEX);
        boolean isNvmeInitiator = rsc.getStateFlags()
            .isSet(apiCtx, Resource.Flags.NVME_INITIATOR);

        Set<StorPool> allStorPools = layerDataHelperProvider.get()
            .getAllNeededStorPools(rsc, payload, layerListRef);

        boolean isStoragePoolShared = areAllShared(allStorPools);

        if ((isNvmeBelow || isOpenflexBelow) && isNvmeInitiator)
        {
            // we need to find our nvme-target resource and copy the node-id from that target-resource
            errorReporter.logTrace("Nvme- / OpenFlex-initiator below us.. looking for target");
            Resource targetRsc = nvmeHelperProvider.get().getTarget(rsc);
            if (targetRsc != null)
            {
                AbsRscLayerObject<Resource> rootLayerData = targetRsc.getLayerData(apiCtx);
                List<AbsRscLayerObject<Resource>> targetDrbdChildren = LayerUtils
                    .getChildLayerDataByKind(rootLayerData, DeviceLayerKind.DRBD);
                for (AbsRscLayerObject<Resource> targetRscData : targetDrbdChildren)
                {
                    if (targetRscData.getResourceNameSuffix().equals(luksRscData.getResourceNameSuffix()))
                    {
                        LuksRscData<Resource> targetLuksRscData = (LuksRscData<Resource>) targetRscData;
                        LuksVlmData<Resource> targetLuksVlmData = targetLuksRscData.getVlmLayerObjects().get(vlm.getVolumeNumber());
                        encryptedVlmKey = targetLuksVlmData.getEncryptedKey();
                        errorReporter.logTrace("encryptedVlmKey found and copied from %s", targetRsc);
                        break;
                    }
                }
            }
        }
        else if (isStoragePoolShared)
        {
            errorReporter.logTrace("searching for encryptedVlmKey in shared resources");
            Set<SharedStorPoolName> sharedSpNames = allStorPools.stream().map(StorPool::getSharedStorPoolName)
                .collect(Collectors.toSet());
            for (Resource otherRsc : sharedRscMgr.getSharedResources(sharedSpNames, rsc.getDefinition()))
            {
                if (otherRsc != rsc)
                {
                    Set<AbsRscLayerObject<Resource>> otherRscLuksDataSet = LayerRscUtils.getRscDataByProvider(
                        otherRsc.getLayerData(apiCtx),
                        DeviceLayerKind.LUKS
                    );
                    for (AbsRscLayerObject<Resource> absOtherRscLayerObject : otherRscLuksDataSet)
                    {
                        LuksRscData<Resource> otherLuksRscData = (LuksRscData<Resource>) absOtherRscLayerObject;
                        if (otherLuksRscData.getResourceNameSuffix().equals(luksRscData.getResourceNameSuffix()))
                        {
                            LuksVlmData<Resource> otherLuksVlmData = otherLuksRscData.getVlmLayerObjects()
                                .get(vlm.getVolumeNumber());
                            encryptedVlmKey = otherLuksVlmData.getEncryptedKey();
                            errorReporter.logTrace("encryptedVlmKey found and copied from %s", otherRsc);
                            break;
                        }
                    }
                }
            }
        }
        // encrypted key was not copied from *-target or sharedRsc.. create new one
        if (encryptedVlmKey == null)
        {
            errorReporter.logTrace("creating new encryptedVlmKey");
            final SecretGenerator secretGen = cryptoProvider.createSecretGenerator();
            encryptedVlmKey = encryptionHelper.encrypt(secretGen.generateSecretString(SECRET_KEY_BYTES));
        }

        return layerDataFactory.createLuksVlmData(
            vlm,
            luksRscData,
            encryptedVlmKey
        );
    }

    @Override
    protected void mergeVlmData(
        LuksVlmData<Resource> vlmDataRef,
        Volume vlmRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
    {
        // nothing to do
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // nothing to do
    }

    @Override
    protected boolean recalculateVolatilePropertiesImpl(
        LuksRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException
    {
        boolean changed = false;
        if (!secObjs.areAllSet())
        {
            changed = setIgnoreReason(rscDataRef, IGNORE_REASON_LUKS_MISSING_KEY, true, false, true);
        }
        return changed;
    }

    @Override
    protected boolean setIgnoreReasonImpl(
        AbsRscLayerObject<Resource> rscDataRef,
        String ignoreReasonRef
    )
        throws DatabaseException
    {
        boolean changed = false;
        if (!IGNORE_REASON_LUKS_MISSING_KEY.equals(ignoreReasonRef))
        {
            changed = super.setIgnoreReasonImpl(rscDataRef, ignoreReasonRef);
        }
        return changed;
    }

    @Override
    protected boolean isExpectedToProvideDevice(LuksRscData<Resource> luksRscData)
    {
        return luksRscData.getIgnoreReason() != null;
    }

    @Override
    protected List<ChildResourceData> getChildRsc(LuksRscData<Resource> rscDataRef, List<DeviceLayerKind> layerListRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return Arrays.asList(new ChildResourceData(RscLayerSuffixes.SUFFIX_DATA));
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<RSC> fromSnapDataRef
    )
    {
        // LuksLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> LuksRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createLuksRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            fromAbsRscDataRef.getResourceNameSuffix(),
            rscParentRef
        );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> VlmDfnLayerObject restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<RSC> fromSnapVlmDataRef
    )
    {
        // LuksLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> LuksVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        LuksRscData<Resource> rscDataRef,
        VlmProviderObject<RSC> vlmProviderObjectRef
    )
        throws DatabaseException
    {
        LuksVlmData<RSC> snapLuksVlmData = (LuksVlmData<RSC>) vlmProviderObjectRef;
        return layerDataFactory.createLuksVlmData(
            vlmRef,
            rscDataRef,
            snapLuksVlmData.getEncryptedKey()
        );
    }
}
