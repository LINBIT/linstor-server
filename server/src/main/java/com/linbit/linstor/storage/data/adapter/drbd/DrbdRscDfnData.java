package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DrbdRscDfnData extends BaseTransactionObject implements DrbdRscDfnObject
{
    // unmodifiable, once initialized
    private final ResourceDefinition rscDfn;
    private final int alStripes;
    private final long alStripeSize;
    private final String suffixedResourceName;
    private final String resourceNameSuffix;
    private final DrbdLayerDatabaseDriver dbDriver;
    private final DynamicNumberPool tcpPortPool;

    // persisted, serialized, ctrl and stlt
    private final TransactionList<DrbdRscDfnData, DrbdRscData> drbdRscDataList;
    private final TransactionMap<VolumeNumber, DrbdVlmDfnData> drbdVlmDfnMap;
    private final TransactionSimpleObject<DrbdRscDfnData, TcpPortNumber> port;
    private final TransactionSimpleObject<DrbdRscDfnData, TransportType> transportType;
    private final TransactionSimpleObject<DrbdRscDfnData, String> secret;
    private final TransactionSimpleObject<DrbdRscDfnData, Short> peerSlots;

    // not persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<DrbdRscDfnData, Boolean> down;

    public DrbdRscDfnData(
        ResourceDefinition rscDfnRef,
        String resourceNameSuffixRef,
        short peerSlotsRef,
        int alStripesRef,
        long alStripesSizeRef,
        Integer portRef,
        TransportType transportTypeRef,
        String secretRef,
        List<DrbdRscData> drbdRscDataListRef,
        Map<VolumeNumber, DrbdVlmDfnData> vlmDfnMap,
        DynamicNumberPool tcpPortPoolRef,
        DrbdLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        super(transMgrProvider);
        resourceNameSuffix = resourceNameSuffixRef;
        tcpPortPool = tcpPortPoolRef;
        dbDriver = dbDriverRef;
        suffixedResourceName = rscDfnRef.getName().displayValue + resourceNameSuffixRef;
        rscDfn = Objects.requireNonNull(rscDfnRef);
        alStripes = alStripesRef;
        alStripeSize = alStripesSizeRef;

        TcpPortNumber tmpPort;
        if (portRef == null)
        {
            tmpPort = new TcpPortNumber(tcpPortPool.autoAllocate());
        }
        else
        {
            tmpPort = new TcpPortNumber(portRef);
            tcpPortPool.allocate(portRef);
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

        drbdVlmDfnMap = transObjFactory.createTransactionMap(vlmDfnMap, null);

        transObjs = Arrays.asList(
            rscDfn,
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
    public ResourceDefinition getResourceDefinition()
    {
        return rscDfn;
    }

    @Override
    public TcpPortNumber getTcpPort()
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
    public TransportType getTransportType()
    {
        return transportType.get();
    }

    public void setTransportType(TransportType typeRef) throws DatabaseException
    {
        transportType.set(typeRef);
    }

    @Override
    public String getSecret()
    {
        return secret.get();
    }

    public void setSecret(String secretRef) throws DatabaseException
    {
        secret.set(secretRef);
    }

    public Collection<DrbdRscData> getDrbdRscDataList()
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
        Collection<DrbdVlmDfnData> drbdVlmDfns = new ArrayList<>(drbdVlmDfnMap.values());
        for (DrbdVlmDfnData drbdVlmDfn : drbdVlmDfns)
        {
            drbdVlmDfn.delete();
        }
        tcpPortPool.deallocate(port.get().value);
        dbDriver.delete(this);
    }


    public void delete(DrbdVlmDfnData drbdVlmDfnDataRef)
    {
        drbdVlmDfnMap.remove(drbdVlmDfnDataRef.getVolumeDefinition().getVolumeNumber());
    }

    public DrbdVlmDfnData getDrbdVlmDfn(VolumeNumber vlmNr)
    {
        return drbdVlmDfnMap.get(vlmNr);
    }

    public void putDrbdVlmDfn(DrbdVlmDfnData drbdVlmDfnDataRef)
    {
        drbdVlmDfnMap.put(drbdVlmDfnDataRef.getVolumeDefinition().getVolumeNumber(), drbdVlmDfnDataRef);
    }

    @Override
    public DrbdRscDfnPojo getApiData(AccessContext accCtxRef)
    {
        return new DrbdRscDfnPojo(
            resourceNameSuffix,
            peerSlots.get(),
            alStripes,
            alStripeSize,
            port.get().value,
            transportType.get().name(),
            secret.get(),
            down.get()
        );
    }
}
