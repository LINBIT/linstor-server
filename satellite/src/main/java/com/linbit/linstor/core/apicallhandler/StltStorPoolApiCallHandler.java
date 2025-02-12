package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolSatelliteFactory;
import com.linbit.linstor.layer.storage.utils.SEDUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

@Singleton
class StltStorPoolApiCallHandler
{
    private static final Set<String> PROP_KEYS_THAT_DO_NOT_COUNT_AS_CHANGE = new HashSet<>();

    static
    {
        PROP_KEYS_THAT_DO_NOT_COUNT_AS_CHANGE.addAll(
            Arrays.asList(
                StorageConstants.NAMESPACE_INTERNAL + StorageConstants.KEY_INT_THIN_POOL_METADATA_PERCENT
            )
        );
    }

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final ControllerPeerConnector controllerPeerConnector;
    private final StorPoolDefinitionSatelliteFactory storPoolDefinitionFactory;
    private final StorPoolSatelliteFactory storPoolFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final ExtCmdFactory extCmdFactory;
    private final DecryptionHelper decryptionHelper;
    private final StltSecurityObjects securityObjects;

    @Inject
    StltStorPoolApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        StorPoolDefinitionSatelliteFactory storPoolDefinitionFactoryRef,
        StorPoolSatelliteFactory storPoolFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        ExtCmdFactory extCmdFactoryRef,
        DecryptionHelper decryptionHelperRef,
        StltSecurityObjects securityObjectsRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        storPoolDfnMap = storPoolDfnMapRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        storPoolDefinitionFactory = storPoolDefinitionFactoryRef;
        storPoolFactory = storPoolFactoryRef;
        transMgrProvider = transMgrProviderRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        extCmdFactory = extCmdFactoryRef;
        decryptionHelper = decryptionHelperRef;
        securityObjects = securityObjectsRef;
    }
    /**
     * We requested an update to a storPool and the controller is telling us that the requested storPool
     * does no longer exist.
     * Basically we now just mark the update as received and applied to prevent the
     * {@link DeviceManager} from waiting for the update.
     *
     * @param storPoolNameStr
     */
    public void applyDeletedStorPool(String storPoolNameStr)
    {
        try
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            StorPoolDefinition removedStorPoolDfn = storPoolDfnMap.remove(storPoolName); // just to be sure
            if (removedStorPoolDfn != null)
            {
                removedStorPoolDfn.delete(apiCtx);
                transMgrProvider.get().commit();
            }

            errorReporter.logInfo("Storage pool definition '" + storPoolNameStr +
                "' and the corresponding storage pool was removed by Controller.");

            Set<StorPoolName> storPoolSet = new TreeSet<>();
            storPoolSet.add(storPoolName);
            deviceManager.storPoolUpdateApplied(storPoolSet, Collections.emptySet(), new ApiCallRcImpl());
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    public @Nullable ChangedData applyChanges(StorPoolPojo storPoolRaw)
    {
        ChangedData changedData = null;
        Set<StorPoolName> storPoolSet = new HashSet<>();
        Set<ResourceName> changedResources = new TreeSet<>();
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            StorPoolName storPoolName;

            StorPoolDefinition storPoolDfnToRegister = null;

            // TODO: uncomment the next line once the localNode gets requested from the controller
            // checkUuid(satellite.localNode, storPoolRaw);

            storPoolName = new StorPoolName(storPoolRaw.getStorPoolName());
            Node localNode = controllerPeerConnector.getLocalNode();
            StorPool storPool;
            if (localNode == null)
            {
                throw new ImplementationError("ApplyChanges called with invalid localnode", new NullPointerException());
            }
            storPool = localNode.getStorPool(apiCtx, storPoolName);

            String action;
            if (storPool != null)
            {
                boolean storPoolChanged = false;
                action = "updated";

                checkUuid(storPool, storPoolRaw);
                checkUuid(storPool.getDefinition(apiCtx), storPoolRaw);

                Props storPoolProps = storPool.getProps(apiCtx);
                Map<String, String> oldSEDMap = SEDUtils.drivePasswordMap(storPoolProps.cloneMap());
                Map<String, String> newSEDMap = SEDUtils.drivePasswordMap(storPoolRaw.getStorPoolProps());
                for (String sedDevice : oldSEDMap.keySet())
                {
                    String oldPassEnc = oldSEDMap.get(sedDevice);
                    if (newSEDMap.containsKey(sedDevice) && !newSEDMap.get(sedDevice).equals(oldPassEnc))
                    {
                        byte[] masterKey = securityObjects.getCryptKey();
                        if (masterKey != null)
                        {
                            // SED password changed
                            String newPassEnc = newSEDMap.get(sedDevice);
                            String oldPass = decryptionHelper.decryptB64ToString(masterKey, oldPassEnc);
                            String newPass = decryptionHelper.decryptB64ToString(masterKey, newPassEnc);
                            String realPath = SEDUtils.realpath(errorReporter, sedDevice);
                            SEDUtils.changeSEDPassword(extCmdFactory, errorReporter, realPath, oldPass, newPass);
                            storPoolChanged = true;
                        }
                        else
                        {
                            throw new LinStorException("Master passphrase missing, cannot change SED password.");
                        }
                    }
                }
                if (mergeProps(storPoolProps, storPoolRaw.getStorPoolProps()))
                {
                    storPoolChanged = true;
                }

                if (storPoolChanged)
                {
                    Collection<VlmProviderObject<Resource>> vlmDataCol = storPool.getVolumes(apiCtx);
                    for (VlmProviderObject<Resource> vlmData : vlmDataCol)
                    {
                        ResourceDefinition rscDfn = vlmData.getVolume().getResourceDefinition();
                        changedResources.add(rscDfn.getName());
                    }

                    Collection<VlmProviderObject<Snapshot>> snapVlmDataCol = storPool
                        .getSnapVolumes(apiCtx);
                    for (VlmProviderObject<Snapshot> snapVlmData : snapVlmDataCol)
                    {
                        SnapshotVolume vlm = (SnapshotVolume) snapVlmData.getVolume();
                        // TODO maybe introduce a changedSnapshot? if that is even possible..
                        changedResources.add(vlm.getResourceName());
                    }
                }
            }
            else
            {
                action = "created";
                StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolName);
                if (storPoolDfn == null)
                {
                    storPoolDfn = storPoolDefinitionFactory.getInstance(
                        apiCtx,
                        storPoolRaw.getStorPoolDfnUuid(),
                        storPoolName
                    );
                    checkUuid(storPoolDfn, storPoolRaw);

                    storPoolDfn.getProps(apiCtx).map().putAll(storPoolRaw.getStorPoolDfnProps());

                    storPoolDfnToRegister = storPoolDfn;
                }

                DeviceProviderKind deviceProviderKind = storPoolRaw.getDeviceProviderKind();
                storPool = storPoolFactory.getInstanceSatellite(
                    apiCtx,
                    storPoolRaw.getStorPoolUuid(),
                    controllerPeerConnector.getLocalNode(),
                    storPoolDfn,
                    deviceProviderKind,
                    freeSpaceMgrFactory.getInstance(
                        SharedStorPoolName.restoreName(storPoolRaw.getFreeSpaceManagerName())
                    ),
                    storPoolRaw.isExternalLocking()
                );

                storPool.getProps(apiCtx).map().putAll(storPoolRaw.getStorPoolProps());
            }

            changedData = new ChangedData(storPoolDfnToRegister);

            if (changedData.storPoolDfnToRegister != null)
            {
                storPoolDfnMap.put(
                    storPoolName,
                    changedData.storPoolDfnToRegister
                );
            }
            transMgrProvider.get().commit();

            errorReporter.logInfo(
                "Storage pool '%s' %s.",
                storPoolName.displayValue,
                action
            );

            storPoolSet.add(storPoolName);

            SpaceInfo spaceInfo = apiCallHandlerUtils.getStoragePoolSpaceInfo(storPool, true);
            DeviceProviderKind kind = storPool.getDeviceProviderKind();
            boolean isFileKind = kind.equals(DeviceProviderKind.FILE) || kind.equals(DeviceProviderKind.FILE_THIN);
            if (spaceInfo != null && (!kind.usesThinProvisioning() || isFileKind))
            {
                boolean supportsSnapshots = storPool.isSnapshotSupported(apiCtx);

                Map<StorPool, SpaceInfo> tmpMap = new HashMap<>();
                tmpMap.put(storPool, spaceInfo);
                controllerPeerConnector.getControllerPeer().sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_NOTIFY_STOR_POOL_APPLIED)
                        .storPoolApplied(storPool, spaceInfo, supportsSnapshots)
                        .build(),
                    InternalApiConsts.API_NOTIFY_STOR_POOL_APPLIED
                );
            }

            responses.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MODIFIED,
                "Changes applied to storage pool '" + storPoolName + "'"
            ));
        }
        catch (LinStorException linStorException)
        {
            errorReporter.reportError(linStorException);
            responses.addEntry(ApiCallRcImpl.copyFromLinstorExc(
                ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR,
                linStorException
            ));
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
            responses.addEntry(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Failed to apply storage pool changes"
                )
                .setCause(exc.getMessage())
                .build()
            );
        }

        deviceManager.storPoolUpdateApplied(storPoolSet, changedResources, responses);

        return changedData;
    }

    private boolean mergeProps(Props storPoolPropsRef, Map<String, String> storPoolApiPropsRef)
    {
        boolean propsChanged = false;
        HashMap<String, String> oldProps = new HashMap<>(storPoolPropsRef.map());
        // just copy the map so we can compare the filtered props for changes
        HashMap<String, String> newProps = new HashMap<>(storPoolApiPropsRef);
        for (String keyThatShouldNotCountAsChange : PROP_KEYS_THAT_DO_NOT_COUNT_AS_CHANGE)
        {
            oldProps.remove(keyThatShouldNotCountAsChange);
            newProps.remove(keyThatShouldNotCountAsChange);
        }
        propsChanged = !oldProps.equals(newProps);

        // even if the "changes" should not count as such (i.e. should not cause a DevMgrRun),
        // we still need to apply them.
        storPoolPropsRef.map().putAll(storPoolApiPropsRef);
        storPoolPropsRef.keySet().retainAll(storPoolApiPropsRef.keySet());

        return propsChanged;
    }

    private void checkUuid(Node node, StorPoolPojo storPoolRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            node.getUuid(),
            storPoolRaw.getNodeUuid(),
            "Node",
            node.getName().displayValue,
            "(unknown)"
        );
    }

    private void checkUuid(StorPool storPool, StorPoolPojo storPoolRaw)
        throws DivergentUuidsException, AccessDeniedException
    {
        checkUuid(
            storPool.getUuid(),
            storPoolRaw.getStorPoolUuid(),
            "StorPool",
            storPool.getDefinition(apiCtx).getName().displayValue,
            storPoolRaw.getStorPoolName()
        );
    }

    private void checkUuid(StorPoolDefinition storPoolDfn, StorPoolPojo storPoolRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            storPoolDfn.getUuid(),
            storPoolRaw.getStorPoolDfnUuid(),
            "StorPoolDefinition",
            storPoolDfn.getName().displayValue,
            storPoolRaw.getStorPoolName()
        );
    }

    private void checkUuid(UUID localUuid, UUID remoteUuid, String type, String localName, String remoteName)
        throws DivergentUuidsException
    {
        if (!localUuid.equals(remoteUuid))
        {
            throw new DivergentUuidsException(
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }

    public static class ChangedData
    {
        @Nullable StorPoolDefinition storPoolDfnToRegister;

        ChangedData(@Nullable StorPoolDefinition storPoolDfnToRegisterRef)
        {
            storPoolDfnToRegister = storPoolDfnToRegisterRef;
        }
    }
}
