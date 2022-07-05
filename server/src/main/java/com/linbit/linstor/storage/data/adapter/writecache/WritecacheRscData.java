package com.linbit.linstor.storage.data.adapter.writecache;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.writecache.WritecacheRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WritecacheRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, WritecacheVlmData<RSC>>
    implements WritecacheRscObject<RSC>
{
    private final WritecacheLayerDatabaseDriver writecacheDbDriver;

    public WritecacheRscData(
        int rscLayerIdRef,
        RSC rscRef,
        AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        String rscNameSuffixRef,
        WritecacheLayerDatabaseDriver dbDriverRef,
        Map<VolumeNumber, WritecacheVlmData<RSC>> vlmProviderObjectsRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscNameSuffixRef,
            dbDriverRef.getIdDriver(),
            vlmProviderObjectsRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        writecacheDbDriver = dbDriverRef;
    }

    @Override
    public RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<WritecacheVlmPojo> vlmPojos = new ArrayList<>();
        for (WritecacheVlmData<RSC> vlmData : vlmMap.values())
        {
            vlmPojos.add(vlmData.asPojo(accCtxRef));
        }
        return new WritecacheRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos,
            suspend.get(),
            ignoreReason.get()
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.WRITECACHE;
    }

    @Override
    protected void deleteVlmFromDatabase(WritecacheVlmData<RSC> vlmRef) throws DatabaseException
    {
        writecacheDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        writecacheDbDriver.delete(this);
    }

}
