package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Base64;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class StltVlmDfnApiCallHandler
{
    private final AccessContext apiCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects secObjs;
    private ResourceDefinitionMap rscDfnMap;
    private DeviceManager devMgr;

    @Inject
    StltVlmDfnApiCallHandler(
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @ApiContext AccessContext apiCtxRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects secObjsRef,
        DeviceManager devMgrRef
    )
    {
        rscDfnMap = rscDfnMapRef;
        apiCtx = apiCtxRef;
        transMgrProvider = transMgrProviderRef;
        secObjs = secObjsRef;
        devMgr = devMgrRef;
    }

    public void decryptAllVlmDfnKeys()
    {
        try
        {
            Set<ResourceName> rscDfnsToAdjust = new TreeSet<>();
            boolean fail = false;
            byte[] masterKey = secObjs.getCryptKey();
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(apiCtx);
                while (vlmDfnIt.hasNext())
                {
                    VolumeDefinition vlmDfn = vlmDfnIt.next();

                    if (isEncrypted(vlmDfn, apiCtx))
                    {
                        rscDfnsToAdjust.add(rscDfn.getName());
                        if (!decryptVlmDfnKeyImpl(vlmDfn, apiCtx, masterKey))
                        {
                            fail = true;
                            break;
                        }
                    }
                }
                if (fail)
                {
                    break;
                }
            }
            TransactionMgr transMgr = transMgrProvider.get();
            if (fail)
            {
                transMgr.rollback();
            }
            else
            {
                transMgr.commit();
                devMgr.getUpdateTracker().checkMultipleResources(rscDfnsToAdjust);
            }
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(
                "SQLException on satellite",
                exc
            );
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Api access context not authorized to perform a required operation",
                accExc
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public static void decryptVlmDfnKey(
        VolumeDefinition vlmDfn,
        byte[] key,
        AccessContext apiCtx
    )
    {
        try
        {
            boolean isEncpryted = isEncrypted(vlmDfn, apiCtx);

            if (key != null && key.length > 0 && isEncpryted)
            {
                decryptVlmDfnKeyImpl(vlmDfn, apiCtx, key);
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Api access context not authorized to perform a required operation",
                accExc
            );
        }
        catch (InvalidKeyException invldKeyExc)
        {
            throw new ImplementationError(
                "Invalid hardcoded key",
                invldKeyExc
            );
        }
    }

    public static boolean isEncrypted(VolumeDefinition vlmDfn, AccessContext apiCtx)
        throws InvalidKeyException, AccessDeniedException
    {
        String encryptedKey = vlmDfn.getProps(apiCtx)
            .getProp(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);
        return encryptedKey != null && encryptedKey.length() > 0;
    }

    private static boolean decryptVlmDfnKeyImpl(
        VolumeDefinition vlmDfn,
        AccessContext apiCtx,
        byte[] masterKey
    )
    {
        boolean success = false;
        try
        {
            Props vlmDfnProps = vlmDfn.getProps(apiCtx);
            String storedVlmKey = vlmDfnProps
                .getProp(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);

            byte[] encryptedVlmKey = Base64.decode(storedVlmKey);

            SymmetricKeyCipher cipher = SymmetricKeyCipher.getInstanceWithKey(masterKey);
            byte[] decryptedVlmKey = cipher.decrypt(encryptedVlmKey);

            vlmDfn.setKey(apiCtx, new String(decryptedVlmKey));

            success = true;
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Api access context not authorized to perform a required operation",
                accExc
            );
        }
        catch (InvalidKeyException invldKeyExc)
        {
            throw new ImplementationError(
                "Invalid hardcoded key",
                invldKeyExc
            );
        }
        catch (LinStorException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(
                "SQLException on satellite",
                exc
            );
        }
        return success;
    }
}
