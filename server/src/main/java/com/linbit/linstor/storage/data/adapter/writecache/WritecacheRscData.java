package com.linbit.linstor.storage.data.adapter.writecache;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.writecache.WritecacheRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WritecacheRscData extends AbsRscData<WritecacheVlmData> implements WritecacheRscObject
{
    public static final String SUFFIX_DATA = "";
    public static final String SUFFIX_CACHE = ".cache";

    private final WritecacheLayerDatabaseDriver writecacheDbDriver;

    public WritecacheRscData(
        int rscLayerIdRef,
        Resource rscRef,
        RscLayerObject parentRef,
        Set<RscLayerObject> childrenRef,
        String rscNameSuffixRef,
        WritecacheLayerDatabaseDriver dbDriverRef,
        Map<VolumeNumber, WritecacheVlmData> vlmProviderObjectsRef,
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
        for (WritecacheVlmData vlmData : vlmMap.values())
        {
            vlmPojos.add(vlmData.asPojo(accCtxRef));
        }
        return new WritecacheRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.WRITECACHE;
    }

    @Override
    protected void deleteVlmFromDatabase(WritecacheVlmData vlmRef) throws DatabaseException
    {
        writecacheDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        writecacheDbDriver.delete(this);
    }

}
