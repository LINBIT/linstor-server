package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DrbdRscData extends AbsRscData<DrbdVlmData> implements DrbdRscObject
{
    public static final String SUFFIX_DATA = "";
    public static final String SUFFIX_META = ".meta"; // . is not a valid character in ResourceName

    // unmodifiable data, once initialized
    private final DrbdRscDfnData drbdRscDfnData;
    private final NodeId nodeId;
    private final short peerSlots;
    private final int alStripes;
    private final long alStripeSize;
    private final DrbdLayerDatabaseDriver drbdDbDriver;

    // persisted, serialized
    private final StateFlags<DrbdRscFlags> flags;

    // not persisted, serialized

    // not persisted, not serialized, stlt only
    private boolean requiresAdjust = false;
    private boolean exists = false;
    private boolean failed = false;
    private boolean isPrimary;
    private boolean isSuspended = false;

    public DrbdRscData(
        int rscLayerIdRef,
        Resource rscRef,
        @Nullable RscLayerObject parentRef,
        DrbdRscDfnData drbdRscDfnDataRef,
        Set<RscLayerObject> childrenRef,
        Map<VolumeNumber, DrbdVlmData> vlmLayerObjectsMapRef,
        String rscNameSuffixRef,
        NodeId nodeIdRef,
        @Nullable Short peerSlotsRef,
        @Nullable Integer alStripesRef,
        @Nullable Long alStripeSizeRef,
        long initFlags,
        DrbdLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscNameSuffixRef,
            dbDriverRef.getIdDriver(),
            vlmLayerObjectsMapRef,
            transObjFactory,
            transMgrProvider
        );

        nodeId = nodeIdRef;
        drbdDbDriver = dbDriverRef;


        flags = transObjFactory.createStateFlagsImpl(
            rsc.getObjProt(),
            this,
            DrbdRscFlags.class,
            dbDriverRef.getRscStateFlagPersistence(),
            initFlags
        );
        drbdRscDfnData = Objects.requireNonNull(drbdRscDfnDataRef);

        peerSlots = peerSlotsRef != null ? peerSlotsRef : drbdRscDfnDataRef.getPeerSlots();
        alStripes = alStripesRef != null ? alStripesRef : drbdRscDfnDataRef.getAlStripes();
        alStripeSize = alStripeSizeRef != null ? alStripeSizeRef : drbdRscDfnDataRef.getAlStripeSize();

        transObjs.add(flags);
        transObjs.add(drbdRscDfnData);
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public boolean hasFailed()
    {
        return failed;
    }

    public void setFailed(boolean failedRef)
    {
        failed = failedRef;
    }

    @Override
    public @Nullable RscLayerObject getParent()
    {
        return parent.get();
    }

    @Override
    public void setParent(@Nonnull RscLayerObject parentObj) throws SQLException
    {
        parent.set(parentObj);
    }

    // no setter for children - unmodifiable after initialized

    @Override
    public NodeId getNodeId()
    {
        return nodeId;
    }

    // no setNodeId - unmodifiable after initialized

    @Override
    public boolean isDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        return flags.isSet(accCtx, DrbdRscFlags.DISKLESS);
    }

    @Override
    public boolean isDisklessForPeers(AccessContext accCtx) throws AccessDeniedException
    {
        return flags.isSet(accCtx, DrbdRscFlags.DISKLESS) &&
            flags.isUnset(accCtx, DrbdRscFlags.DISK_ADDING) &&
            flags.isUnset(accCtx, DrbdRscFlags.DISK_REMOVING);
    }

    @Override
    public DrbdRscDfnData getRscDfnLayerObject()
    {
        return drbdRscDfnData;
    }

    // no setter for drbdRscDfnData - unmodifiable after initialized

    public void putVlmLayerObject(DrbdVlmData data)
    {
        vlmMap.put(data.getVlmNr(), data);
    }

    public StateFlags<DrbdRscFlags> getFlags()
    {
        return flags;
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

    @Override
    public void delete() throws SQLException
    {
        super.delete();
        drbdRscDfnData.getDrbdRscDataList().remove(this);
    }

    @Override
    protected void deleteVlmFromDatabase(DrbdVlmData drbdVlmData) throws SQLException
    {
        drbdDbDriver.delete(drbdVlmData);
    }

    @Override
    protected void deleteRscFromDatabase() throws SQLException
    {
        drbdDbDriver.delete(this);
    }

    /*
     * Temporary data - will not be persisted
     */

    public boolean exists()
    {
        return exists;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
    }

    public boolean isAdjustRequired()
    {
        return requiresAdjust;
    }

    public void setAdjustRequired(boolean requiresAdjustRef)
    {
        requiresAdjust = requiresAdjustRef;
    }

    public boolean isPrimary()
    {
        return isPrimary;
    }

    public void setPrimary(boolean isPrimaryRef)
    {
        isPrimary = isPrimaryRef;
    }

    public boolean isSuspended()
    {
        return isSuspended;
    }

    public void setSuspended(boolean isSuspendedRef)
    {
        isSuspended = isSuspendedRef;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtx) throws AccessDeniedException
    {
        List<DrbdVlmPojo> vlmPojos = new ArrayList<>();
        for (DrbdVlmData drbdVlmData : vlmMap.values())
        {
            vlmPojos.add(drbdVlmData.asPojo(accCtx));
        }
        return new DrbdRscPojo(
            rscLayerId,
            getChildrenPojos(accCtx),
            getResourceNameSuffix(),
            drbdRscDfnData.getApiData(accCtx),
            nodeId.value,
            peerSlots,
            alStripes,
            alStripeSize,
            flags.getFlagsBits(accCtx),
            vlmPojos
        );
    }
}
