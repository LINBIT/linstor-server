package com.linbit.linstor.storage.data.provider;

import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StorageRscData extends AbsRscData<VlmProviderObject>
{
    private StorageLayerDatabaseDriver dbDriver;

    public StorageRscData(
        int rscLayerIdRef,
        RscLayerObject parentRef,
        Resource rscRef,
        String rscNameSuffixRef,
        Map<VolumeNumber, VlmProviderObject> vlmProviderObjectsRef,
        StorageLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            Collections.emptySet(), // no children
            rscNameSuffixRef,
            dbDriverRef.getIdDriver(),
            vlmProviderObjectsRef,
            transObjFactory,
            transMgrProvider
        );
        dbDriver = dbDriverRef;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    protected void deleteVlmFromDatabase(VlmProviderObject vlmRef) throws SQLException
    {
        dbDriver.delete(vlmRef);
    }

    @Override
    public RscLayerDataPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<VlmLayerDataPojo> vlmPojos = new ArrayList<>();
        for (VlmProviderObject vlmProviderObject : vlmMap.values())
        {
            vlmPojos.add(vlmProviderObject.asPojo(accCtxRef));
        }
        return new StorageRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos
        );
    }
}
