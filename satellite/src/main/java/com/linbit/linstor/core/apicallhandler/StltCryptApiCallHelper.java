package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.storage.utils.SEDUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class StltCryptApiCallHelper
{
    private final AccessContext apiCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects secObjs;
    private final ResourceDefinitionMap rscDfnMap;
    private final DeviceManager devMgr;
    private final DecryptionHelper decryptionHelper;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    StltCryptApiCallHelper(
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @ApiContext AccessContext apiCtxRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects secObjsRef,
        DeviceManager devMgrRef,
        DecryptionHelper decryptionHelperRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        rscDfnMap = rscDfnMapRef;
        apiCtx = apiCtxRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
        devMgr = devMgrRef;
        decryptionHelper = decryptionHelperRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
    }

    private Set<ResourceName> findResourcesUsingStorPool(StorPoolName storPoolName) throws AccessDeniedException
    {
        final Set<ResourceName> resources = new TreeSet<>();
        for (ResourceDefinition rscDfn : rscDfnMap.values())
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();

                AbsRscLayerObject<Resource> layerData = rsc.getLayerData(apiCtx);
                List<AbsRscLayerObject<Resource>> rscDataList = LayerUtils.getChildLayerDataByKind(
                    layerData,
                    DeviceLayerKind.STORAGE
                );

                for (AbsRscLayerObject<Resource> rscData : rscDataList)
                {
                    for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
                    {
                        if (vlmData.getStorPool().getName().equals(storPoolName))
                        {
                            resources.add(rscDfn.getName());
                        }
                    }
                }
            }
        }
        return resources;
    }

    private Set<ResourceName> decryptSEDDrives(final byte[] masterKey) throws LinStorException
    {
        final Set<ResourceName> updateResources = new TreeSet<>();
        Node localNode = controllerPeerConnector.getLocalNode();
        Iterator<StorPool> itStorPool = localNode.iterateStorPools(apiCtx);
        while (itStorPool.hasNext())
        {
            StorPool sp = itStorPool.next();
            @Nullable Props sedNS = sp.getProps(apiCtx).getNamespace(ApiConsts.NAMESPC_SED);
            // SED namespace contains drives as keys with their password as value
            if (sedNS != null)
            {
                Map<String, String> sedMap = SEDUtils.drivePasswordMap(sedNS.cloneMap());
                for (final String drive : sedMap.keySet())
                {
                    String sedEncPassword = sedMap.get(drive);
                    String sedPassword = decryptionHelper.decryptB64ToString(masterKey, sedEncPassword);
                    SEDUtils.unlockSED(extCmdFactory, errorReporter, drive, sedPassword);
                }

                updateResources.addAll(findResourcesUsingStorPool(sp.getName()));
            }
        }
        return updateResources;
    }

    private Set<ResourceName> decryptLuksVlmKeys(final byte[] masterKey) throws LinStorException
    {
        final Set<ResourceName> decryptedResources = new TreeSet<>();
        for (ResourceDefinition rscDfn : rscDfnMap.values())
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                AbsRscLayerObject<Resource> layerData = rsc.getLayerData(apiCtx);
                List<AbsRscLayerObject<Resource>> rscDataList = LayerUtils.getChildLayerDataByKind(
                    layerData,
                    DeviceLayerKind.LUKS
                );

                boolean reactivate = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.REACTIVATE);
                for (AbsRscLayerObject<Resource> rscData : rscDataList)
                {
                    for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
                    {
                        LuksVlmData<Resource> cryptVlmData = (LuksVlmData<Resource>) vlmData;
                        if (reactivate || cryptVlmData.getDecryptedPassword() == null)
                        {
                            byte[] encryptedKey = cryptVlmData.getEncryptedKey();
                            byte[] decryptedKey = decryptionHelper.decrypt(masterKey, encryptedKey);

                            cryptVlmData.setDecryptedPassword(decryptedKey);
                            decryptedResources.add(rscDfn.getName());
                        }
                    }
                }
            }
        }
        return decryptedResources;
    }

    /**
     * Although this method does not decrypt anything (decryption is done in the EBS Providers), we sill want to include
     * the EBS resources in the next deviceManager run
     *
     * @throws AccessDeniedException
     */
    private Collection<? extends ResourceName> getAllEBSResources() throws AccessDeniedException
    {
        Set<ResourceName> resourcesToProcess = new HashSet<>();
        for (ResourceDefinition rscDfn : rscDfnMap.values())
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {

                Resource rsc = rscIt.next();
                AbsRscLayerObject<Resource> layerData = rsc.getLayerData(apiCtx);
                // also add EBS resources to the new devMgr run
                List<AbsRscLayerObject<Resource>> storRscData = LayerUtils.getChildLayerDataByKind(
                    layerData,
                    DeviceLayerKind.STORAGE
                );
                for (AbsRscLayerObject<Resource> rscData : storRscData)
                {
                    for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
                    {
                        DeviceProviderKind deviceProviderKind = vlmData.getStorPool().getDeviceProviderKind();
                        if (deviceProviderKind.equals(DeviceProviderKind.EBS_INIT) ||
                            deviceProviderKind.equals(DeviceProviderKind.EBS_TARGET))
                        {
                            resourcesToProcess.add(rscDfn.getName());
                        }
                    }
                }
            }
        }
        return resourcesToProcess;
    }

    public void decryptVolumesAndDrives(boolean updateDevMgr)
    {
        try
        {
            byte[] masterKey = secObjs.getCryptKey();

            if (masterKey != null)
            {
                final Set<ResourceName> decryptedResources = new TreeSet<>();

                decryptedResources.addAll(decryptSEDDrives(masterKey));
                decryptedResources.addAll(decryptLuksVlmKeys(masterKey));
                decryptedResources.addAll(getAllEBSResources());

                transMgrProvider.get().commit();
                if (updateDevMgr)
                {
                    devMgr.forceWakeUpdateNotifications();
                    devMgr.markMultipleResourcesForDispatch(decryptedResources);
                }
            }
        }
        catch (LinStorException | TransactionException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}
