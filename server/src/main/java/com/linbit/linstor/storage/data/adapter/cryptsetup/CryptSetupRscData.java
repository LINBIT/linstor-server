package com.linbit.linstor.storage.data.adapter.cryptsetup;

import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo.CryptVlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.cryptsetup.CryptSetupRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CryptSetupRscData extends AbsRscData<CryptSetupVlmData> implements CryptSetupRscObject
{
    public CryptSetupRscData(
        int rscLayerIdRef,
        Resource rscRef,
        String rscNameSuffixRef,
        @Nullable RscLayerObject parentRef,
        Set<RscLayerObject> childrenRef,
        Map<VolumeNumber, CryptSetupVlmData> vlmLayerObjectsRef,
        CryptSetupLayerDatabaseDriver dbDriver,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscNameSuffixRef,
            dbDriver.getIdDriver(),
            vlmLayerObjectsRef,
            transObjFactory,
            transMgrProvider
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.CRYPT_SETUP;
    }

    @Override
    public @Nullable RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerDataPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<CryptVlmPojo> vlmPojos = new ArrayList<>();
        for (CryptSetupVlmData cryptSetupVlmData : vlmMap.values())
        {
            vlmPojos.add(cryptSetupVlmData.asPojo(accCtxRef));
        }
        return new CryptSetupRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos
        );
    }
}
