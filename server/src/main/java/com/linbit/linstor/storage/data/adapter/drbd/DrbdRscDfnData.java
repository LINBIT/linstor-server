package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class DrbdRscDfnData extends BaseTransactionObject implements DrbdRscDfnObject
{
    // unmodifiable, once initialized
    private final ResourceDefinition rscDfn;
    private final short peerSlots;
    private final int alStripes;
    private final long alStripeSize;
    private final String suffixedResourceName;
    private final String resourceNameSuffix;
    private final DrbdLayerDatabaseDriver dbDriver;

    // persisted, serialized, ctrl and stlt
    private final TransactionList<DrbdRscDfnData, DrbdRscData> drbdRscDataList;
    private final TransactionSimpleObject<DrbdRscDfnData, TcpPortNumber> port;
    private final TransactionSimpleObject<DrbdRscDfnData, TransportType> transportType;
    private final TransactionSimpleObject<DrbdRscDfnData, String> secret;

    public DrbdRscDfnData(
        ResourceDefinition rscDfnRef,
        String resourceNameSuffixRef,
        short peerSlotsRef,
        int alStripesRef,
        long alStripesSizeRef,
        TcpPortNumber portRef,
        TransportType transportTypeRef,
        String secretRef,
        List<DrbdRscData> drbdRscDataListRef,
        DrbdLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        resourceNameSuffix = resourceNameSuffixRef;
        dbDriver = dbDriverRef;
        suffixedResourceName = rscDfnRef.getName().displayValue + resourceNameSuffixRef;
        rscDfn = Objects.requireNonNull(rscDfnRef);
        peerSlots = peerSlotsRef;
        alStripes = alStripesRef;
        alStripeSize = alStripesSizeRef;
        port = transObjFactory.createTransactionSimpleObject(this, portRef, dbDriverRef.getTcpPortDriver());
        transportType = transObjFactory.createTransactionSimpleObject(
            this,
            transportTypeRef,
            dbDriverRef.getTransportTypeDriver()
        );
        secret = transObjFactory.createTransactionSimpleObject(this, secretRef, dbDriverRef.getRscDfnSecretDriver());
        drbdRscDataList = transObjFactory.createTransactionList(this, drbdRscDataListRef, null);

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

    public void setPort(TcpPortNumber portRef) throws SQLException
    {
        port.set(portRef);
    }

    @Override
    public TransportType getTransportType()
    {
        return transportType.get();
    }

    public void setTransportType(TransportType typeRef) throws SQLException
    {
        transportType.set(typeRef);
    }

    @Override
    public String getSecret()
    {
        return secret.get();
    }

    public void setSecret(String secretRef) throws SQLException
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
        return peerSlots;
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
    public void delete() throws SQLException
    {
        dbDriver.delete(this);
    }

    public DrbdRscDfnPojo asPojo(AccessContext accCtxRef)
    {
        return new DrbdRscDfnPojo(
            suffixedResourceName,
            peerSlots,
            alStripes,
            alStripeSize,
            port.get().value,
            transportType.get().name(),
            secret.get()
        );
    }

}
