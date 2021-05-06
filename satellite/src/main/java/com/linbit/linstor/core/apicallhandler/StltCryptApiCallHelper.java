package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class StltCryptApiCallHelper
{
    private final AccessContext apiCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects secObjs;
    private ResourceDefinitionMap rscDfnMap;
    private DeviceManager devMgr;
    private final DecryptionHelper decryptionHelper;

    @Inject
    StltCryptApiCallHelper(
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @ApiContext AccessContext apiCtxRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects secObjsRef,
        DeviceManager devMgrRef,
        DecryptionHelper decryptionHelperRef
    )
    {
        rscDfnMap = rscDfnMapRef;
        apiCtx = apiCtxRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
        devMgr = devMgrRef;
        decryptionHelper = decryptionHelperRef;
    }

    public void decryptAllNewLuksVlmKeys(boolean updateDevMgr)
    {
        try
        {
            Set<ResourceName> decryptedResources = new TreeSet<>();

            byte[] masterKey = secObjs.getCryptKey();
            if (masterKey != null)
            {
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
