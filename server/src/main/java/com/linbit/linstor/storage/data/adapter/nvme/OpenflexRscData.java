package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.OpenflexRscPojo;
import com.linbit.linstor.api.pojo.OpenflexRscPojo.OpenflexVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.nvme.OpenflexRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OpenflexRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, OpenflexVlmData<RSC>>
    implements OpenflexRscObject<RSC>
{
    // unmodifiable data, once initialized
    private final OpenflexLayerDatabaseDriver ofDbDriver;
    private final OpenflexRscDfnData<RSC> rscDfnData;

    private final TransactionSimpleObject<OpenflexRscData<?>, Boolean> inUse;

    private boolean exists;

    public OpenflexRscData(
        int rscLayerIdRef,
        RSC rscRef,
        OpenflexRscDfnData<RSC> rscDfnDataRef,
        AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        Map<VolumeNumber, OpenflexVlmData<RSC>> vlmProviderObjectsRef,
        OpenflexLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscDfnDataRef.getRscNameSuffix(),
            dbDriverRef.getIdDriver(),
            vlmProviderObjectsRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        rscDfnData = rscDfnDataRef;
        ofDbDriver = dbDriverRef;

        inUse = transObjFactoryRef.createTransactionSimpleObject(this, null, null);

        transObjs = Arrays.asList(
            inUse
        );
    }

    public boolean isInitiator(AccessContext accCtx) throws AccessDeniedException
    {
        boolean isInitiator = false;
        if (rsc instanceof Resource)
        {
            isInitiator = ((Resource) rsc).getStateFlags().isSet(accCtx, Resource.Flags.NVME_INITIATOR);
        }
        return isInitiator;
    }

    public Boolean isInUse()
    {
        return inUse.get();
    }

    public void setInUse(Boolean inUseRef) throws DatabaseException
    {
        inUse.set(inUseRef);
    }

    @Override
    public OpenflexRscDfnData<RSC> getRscDfnLayerObject()
    {
        return rscDfnData;
    }

    public boolean exists()
    {
        return exists;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.OPENFLEX;
    }

    @Override
    protected void deleteVlmFromDatabase(OpenflexVlmData<RSC> vlmRef) throws DatabaseException
    {
        ofDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        ofDbDriver.delete(this);
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<OpenflexVlmPojo> vlmPojos = new ArrayList<>();
        for (OpenflexVlmData<RSC> ofVlmData : vlmMap.values())
        {
            vlmPojos.add(ofVlmData.asPojo(accCtxRef));
        }
        return new OpenflexRscPojo(
            rscLayerId,
            rscDfnData.getApiData(accCtxRef),
            vlmPojos,
            suspend.get(),
            ignoreReason.get()
        );
    }
}
