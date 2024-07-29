package com.linbit.linstor.storage.data.adapter.writecache;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
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
    private final LayerWritecacheRscDatabaseDriver writecacheRscDbDriver;
    private final LayerWritecacheVlmDatabaseDriver writecacheVlmDbDriver;

    public WritecacheRscData(
        int rscLayerIdRef,
        RSC rscRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        String rscNameSuffixRef,
        LayerWritecacheRscDatabaseDriver writecacheRscDbDriverRef,
        LayerWritecacheVlmDatabaseDriver writecacheVlmDbDriverRef,
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
            writecacheRscDbDriverRef.getIdDriver(),
            vlmProviderObjectsRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        writecacheRscDbDriver = writecacheRscDbDriverRef;
        writecacheVlmDbDriver = writecacheVlmDbDriverRef;
    }

    @Override
    public @Nullable RscDfnLayerObject getRscDfnLayerObject()
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
            ignoreReasons.get()
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
        writecacheVlmDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        writecacheRscDbDriver.delete(this);
    }

}
