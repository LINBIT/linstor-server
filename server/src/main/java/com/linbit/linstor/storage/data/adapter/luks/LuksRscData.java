package com.linbit.linstor.storage.data.adapter.luks;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.luks.LuksRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LuksRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, LuksVlmData<RSC>>
    implements LuksRscObject<RSC>
{
    private final LayerLuksRscDatabaseDriver luksRscDbDriver;
    private final LayerLuksVlmDatabaseDriver luksVlmDbDriver;

    public LuksRscData(
        int rscLayerIdRef,
        RSC rscRef,
        String rscNameSuffixRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        Map<VolumeNumber, LuksVlmData<RSC>> vlmLayerObjectsRef,
        LayerLuksRscDatabaseDriver luksRscDbDriverRef,
        LayerLuksVlmDatabaseDriver luksVlmDbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscNameSuffixRef,
            luksRscDbDriverRef.getIdDriver(),
            vlmLayerObjectsRef,
            transObjFactory,
            transMgrProvider
        );
        luksRscDbDriver = luksRscDbDriverRef;
        luksVlmDbDriver = luksVlmDbDriverRef;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.LUKS;
    }

    @Override
    public @Nullable RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    protected void deleteVlmFromDatabase(LuksVlmData<RSC> vlmRef) throws DatabaseException
    {
        luksVlmDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        luksRscDbDriver.delete(this);
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<LuksVlmPojo> vlmPojos = new ArrayList<>();
        for (LuksVlmData<RSC> luksVlmData : vlmMap.values())
        {
            vlmPojos.add(luksVlmData.asPojo(accCtxRef));
        }
        return new LuksRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos,
            suspend.get(),
            ignoreReasons.get()
        );
    }
}
