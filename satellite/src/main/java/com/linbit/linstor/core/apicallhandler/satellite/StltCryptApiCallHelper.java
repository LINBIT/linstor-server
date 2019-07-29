package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.crypto.LengthPadding;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionMgr;

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
    private final LengthPadding cryptoLenPad;

    @Inject
    StltCryptApiCallHelper(
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @ApiContext AccessContext apiCtxRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects secObjsRef,
        DeviceManager devMgrRef,
        LengthPadding cryptoLenPadRef
    )
    {
        rscDfnMap = rscDfnMapRef;
        apiCtx = apiCtxRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
        devMgr = devMgrRef;
        cryptoLenPad = cryptoLenPadRef;
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
                        RscLayerObject layerData = rsc.getLayerData(apiCtx);
                        List<RscLayerObject> rscDataList = LayerUtils.getChildLayerDataByKind(
                            layerData,
                            DeviceLayerKind.LUKS
                        );

                        for (RscLayerObject rscData : rscDataList)
                        {
                            for (VlmProviderObject vlmData : rscData.getVlmLayerObjects().values())
                            {
                                LuksVlmData cryptVlmData = (LuksVlmData) vlmData;
                                if (cryptVlmData.getDecryptedPassword() == null)
                                {
                                    byte[] encryptedKey = cryptVlmData.getEncryptedKey();
                                    SymmetricKeyCipher cipher = SymmetricKeyCipher.getInstanceWithKey(masterKey);
                                    byte[] decryptedData = cipher.decrypt(encryptedKey);
                                    byte[] decryptedKey = cryptoLenPad.retrieve(decryptedData);

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
