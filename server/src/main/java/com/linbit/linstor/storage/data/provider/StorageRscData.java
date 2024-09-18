package com.linbit.linstor.storage.data.provider;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StorageRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, VlmProviderObject<RSC>>
{
    private final LayerStorageRscDatabaseDriver storageRscDbDriver;
    private final LayerStorageVlmDatabaseDriver storageVlmDbDriver;

    public StorageRscData(
        int rscLayerIdRef,
        AbsRscLayerObject<RSC> parentRef,
        RSC rscRef,
        String rscNameSuffixRef,
        Map<VolumeNumber, VlmProviderObject<RSC>> vlmProviderObjectsRef,
        LayerStorageRscDatabaseDriver dbDriverRef,
        LayerStorageVlmDatabaseDriver dbVlmDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
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
        storageRscDbDriver = dbDriverRef;
        storageVlmDbDriver = dbVlmDriverRef;
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
    protected void deleteVlmFromDatabase(VlmProviderObject<RSC> vlmRef) throws DatabaseException
    {
        storageVlmDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        storageRscDbDriver.delete(this);
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<VlmLayerDataApi> vlmPojos = new ArrayList<>();
        for (VlmProviderObject<RSC> vlmProviderObject : vlmMap.values())
        {
            vlmPojos.add(vlmProviderObject.asPojo(accCtxRef));
        }
        return new StorageRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos,
            suspend.get(),
            ignoreReasons.get()
        );
    }
}
