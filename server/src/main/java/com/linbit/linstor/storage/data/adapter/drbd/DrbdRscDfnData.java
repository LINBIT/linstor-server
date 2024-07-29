package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DrbdRscDfnData<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements DrbdRscDfnObject
{
    public static final int SNAPSHOT_TCP_PORT = -1;

    // unmodifiable, once initialized
    private final ResourceName rscName;
    private final @Nullable SnapshotName snapName;
    private final int alStripes;
    private final long alStripeSize;
    private final String suffixedResourceName;
    private final String resourceNameSuffix;
    private final LayerDrbdRscDfnDatabaseDriver dbDriver;
    private final DynamicNumberPool tcpPortPool;

    // persisted, serialized, ctrl and stlt
    private final TransactionList<DrbdRscDfnData<RSC>, DrbdRscData<RSC>> drbdRscDataList;
    private final TransactionMap<DrbdRscDfnData<?>, VolumeNumber, DrbdVlmDfnData<RSC>> drbdVlmDfnMap;
    private final TransactionSimpleObject<DrbdRscDfnData<?>, @Nullable TcpPortNumber> port;
    private final TransactionSimpleObject<DrbdRscDfnData<?>, @Nullable TransportType> transportType;
    private final TransactionSimpleObject<DrbdRscDfnData<?>, @Nullable String> secret;
    private final TransactionSimpleObject<DrbdRscDfnData<?>, Short> peerSlots;

    // not persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<DrbdRscDfnData<RSC>, Boolean> down;

    public DrbdRscDfnData(
        ResourceName rscNameRef,
        @Nullable SnapshotName snapNameRef,
        String resourceNameSuffixRef,
        short peerSlotsRef,
        int alStripesRef,
        long alStripesSizeRef,
        Integer portRef,
        @Nullable TransportType transportTypeRef,
        @Nullable String secretRef,
        List<DrbdRscData<RSC>> drbdRscDataListRef,
        Map<VolumeNumber, DrbdVlmDfnData<RSC>> vlmDfnMap,
        DynamicNumberPool tcpPortPoolRef,
        LayerDrbdRscDfnDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        super(transMgrProvider);
        resourceNameSuffix = resourceNameSuffixRef;
        tcpPortPool = tcpPortPoolRef;
        dbDriver = dbDriverRef;
        suffixedResourceName = rscNameRef.displayValue + (snapNameRef == null ? "" : snapNameRef.displayValue) +
            resourceNameSuffixRef;
        rscName = Objects.requireNonNull(rscNameRef);
        snapName = snapNameRef;
        alStripes = alStripesRef;
        alStripeSize = alStripesSizeRef;

        TcpPortNumber tmpPort;
        if (portRef == null)
        {
            tmpPort = new TcpPortNumber(tcpPortPool.autoAllocate());
        }
        else
        {
            if (portRef == SNAPSHOT_TCP_PORT)
            {
                tmpPort = null;
                if (snapNameRef == null)
                {
                    throw new ImplementationError("Invalid port number given for resource");
                }
            }
            else
            {
                tmpPort = new TcpPortNumber(portRef);
                tcpPortPool.allocate(portRef);
            }
        }
        port = transObjFactory.createTransactionSimpleObject(this, tmpPort, dbDriverRef.getTcpPortDriver());
        transportType = transObjFactory.createTransactionSimpleObject(
            this,
            transportTypeRef,
            dbDriverRef.getTransportTypeDriver()
        );
        secret = transObjFactory.createTransactionSimpleObject(this, secretRef, dbDriverRef.getRscDfnSecretDriver());
        drbdRscDataList = transObjFactory.createTransactionList(this, drbdRscDataListRef, null);
        peerSlots = transObjFactory.createTransactionSimpleObject(this, peerSlotsRef, dbDriverRef.getPeerSlotsDriver());

        down = transObjFactory.createTransactionSimpleObject(this, false, null);

        drbdVlmDfnMap = transObjFactory.createTransactionMap(this, vlmDfnMap, null);

        transObjs = Arrays.asList(
            port,
            transportType,
            secret,
            drbdRscDataList
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public ResourceName getResourceName()
    {
        return rscName;
    }

    @Override
    public @Nullable SnapshotName getSnapshotName()
    {
        return snapName;
    }

    @Override
    public @Nullable TcpPortNumber getTcpPort()
    {
        return port.get();
    }

    public void setPort(Integer portRef)
        throws ExhaustedPoolException, DatabaseException, ValueOutOfRangeException, ValueInUseException
    {
        tcpPortPool.deallocate(port.get().value);
        int actualPort = portRef == null ? tcpPortPool.autoAllocate() : portRef;
        port.set(new TcpPortNumber(actualPort));
        tcpPortPool.allocate(actualPort);
    }

    public void setPort(TcpPortNumber portRef) throws DatabaseException, ValueInUseException
    {
        tcpPortPool.deallocate(port.get().value);
        port.set(portRef);
        tcpPortPool.allocate(portRef.value);
    }

    @Override
    public @Nullable TransportType getTransportType()
    {
        return transportType.get();
    }

    public void setTransportType(@Nullable TransportType typeRef) throws DatabaseException
    {
        transportType.set(typeRef);
    }

    @Override
    public @Nullable String getSecret()
    {
        return secret.get();
    }

    public void setSecret(@Nullable String secretRef) throws DatabaseException
    {
        secret.set(secretRef);
    }

    public Collection<DrbdRscData<RSC>> getDrbdRscDataList()
    {
        return drbdRscDataList;
    }

    @Override
    public short getPeerSlots()
    {
        return peerSlots.get();
    }

    public void setPeerSlots(short peerSlotsRef) throws DatabaseException
    {
        peerSlots.set(peerSlotsRef);
    }

    @Override
    public int getAlStripes()
    {
        return alStripes;
    }

    @Override
    public long getAlStripeSize()
    {
        return alStripeSize;
    }

    @Override
    public void setDown(boolean downRef) throws DatabaseException
    {
        down.set(downRef);
    }

    @Override
    public boolean isDown()
    {
        return down.get();
    }

    public String getSuffixedResourceName()
    {
        return suffixedResourceName;
    }

    @Override
    public String getRscNameSuffix()
    {
        return resourceNameSuffix;
    }

    @Override
    public void delete() throws DatabaseException
    {
        Collection<DrbdVlmDfnData<RSC>> drbdVlmDfns = new ArrayList<>(drbdVlmDfnMap.values());
        for (DrbdVlmDfnData<RSC> drbdVlmDfn : drbdVlmDfns)
        {
            drbdVlmDfn.delete();
        }
        TcpPortNumber tcpPortNumber = port.get();
        if (tcpPortNumber != null)
        {
            tcpPortPool.deallocate(tcpPortNumber.value);
        }
        dbDriver.delete(this);
    }

    public void delete(DrbdVlmDfnData<RSC> drbdVlmDfnDataRef)
    {
        drbdVlmDfnMap.remove(drbdVlmDfnDataRef.getVolumeNumber());
    }

    public DrbdVlmDfnData<RSC> getDrbdVlmDfn(VolumeNumber vlmNr)
    {
        return drbdVlmDfnMap.get(vlmNr);
    }

    public void putDrbdVlmDfn(DrbdVlmDfnData<RSC> drbdVlmDfnDataRef)
    {
        drbdVlmDfnMap.put(drbdVlmDfnDataRef.getVolumeNumber(), drbdVlmDfnDataRef);
    }

    @Override
    public DrbdRscDfnPojo getApiData(AccessContext accCtxRef)
    {
        TcpPortNumber tcpPort = port.get();
        return new DrbdRscDfnPojo(
            resourceNameSuffix,
            peerSlots.get(),
            alStripes,
            alStripeSize,
            tcpPort == null ? null : tcpPort.value,
            transportType.get().name(),
            secret.get(),
            down.get()
        );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resourceNameSuffix, rscName, snapName);
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof DrbdRscDfnData)
        {
            DrbdRscDfnData<?> other = (DrbdRscDfnData<?>) obj;
            ret = Objects.equals(resourceNameSuffix, other.resourceNameSuffix) &&
                Objects.equals(rscName, other.rscName) && Objects.equals(snapName, other.snapName);
        }
        return ret;
    }
}
