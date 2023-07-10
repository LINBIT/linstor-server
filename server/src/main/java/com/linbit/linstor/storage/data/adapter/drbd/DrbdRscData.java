package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DrbdRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, DrbdVlmData<RSC>>
    implements DrbdRscObject<RSC>, Comparable<DrbdRscData<RSC>>
{
    // unmodifiable data, once initialized
    private final DrbdRscDfnData<RSC> drbdRscDfnData;
    private final TransactionSimpleObject<DrbdRscData<?>, NodeId> nodeId;
    private final short peerSlots;
    private final int alStripes;
    private final long alStripeSize;
    private final LayerDrbdRscDatabaseDriver drbdRscDbDriver;
    private final LayerDrbdVlmDatabaseDriver drbdVlmDbDriver;

    // persisted, serialized
    private final StateFlags<DrbdRscFlags> flags;

    // not persisted, serialized
    private @Nullable Integer promotionScore = null;
    private @Nullable Boolean mayPromote = null;

    // not persisted, not serialized, stlt only
    private boolean requiresAdjust = false;
    private boolean exists = false;
    private boolean failed = false;
    private boolean isPrimary;
    private boolean isSuspended = false;
    private boolean resFileExists = false;

    public DrbdRscData(
        int rscLayerIdRef,
        RSC rscRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        DrbdRscDfnData<RSC> drbdRscDfnDataRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        Map<VolumeNumber, DrbdVlmData<RSC>> vlmLayerObjectsMapRef,
        String rscNameSuffixRef,
        NodeId nodeIdRef,
        @Nullable Short peerSlotsRef,
        @Nullable Integer alStripesRef,
        @Nullable Long alStripeSizeRef,
        long initFlags,
        LayerDrbdRscDatabaseDriver drbdRscDbDriverRef,
        LayerDrbdVlmDatabaseDriver drbdVlmDbDriverRef,
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
            drbdRscDbDriverRef.getIdDriver(),
            vlmLayerObjectsMapRef,
            transObjFactory,
            transMgrProvider
        );

        nodeId = transObjFactory.createTransactionSimpleObject(this, nodeIdRef, drbdRscDbDriverRef.getNodeIdDriver());
        drbdRscDbDriver = drbdRscDbDriverRef;
        drbdVlmDbDriver = drbdVlmDbDriverRef;

        flags = transObjFactory.createStateFlagsImpl(
            rscRef.getObjProt(),
            this,
            DrbdRscFlags.class,
            drbdRscDbDriverRef.getRscStateFlagPersistence(),
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
    public @Nullable AbsRscLayerObject<RSC> getParent()
    {
        return parent.get();
    }

    @Override
    public void setParent(@Nonnull AbsRscLayerObject<RSC> parentObj) throws DatabaseException
    {
        parent.set(parentObj);
    }

    // no setter for children - unmodifiable after initialized

    @Override
    public NodeId getNodeId()
    {
        return nodeId.get();
    }

    @Override
    public void setNodeId(NodeId nodeIdRef) throws DatabaseException
    {
        nodeId.set(nodeIdRef);
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
    public DrbdRscDfnData<RSC> getRscDfnLayerObject()
    {
        return drbdRscDfnData;
    }

    // no setter for drbdRscDfnData - unmodifiable after initialized

    public void putVlmLayerObject(DrbdVlmData<RSC> data)
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
    public void delete(AccessContext accCtxRef) throws DatabaseException, AccessDeniedException
    {
        super.delete(accCtxRef);
        drbdRscDfnData.getDrbdRscDataList().remove(this);
    }

    @Override
    protected void deleteVlmFromDatabase(DrbdVlmData<RSC> drbdVlmData) throws DatabaseException
    {
        drbdVlmDbDriver.delete(drbdVlmData);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        drbdRscDbDriver.delete(this);
    }

    /*
     * Temporary data - will not be persisted
     */

    @Override
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

    public @Nullable Boolean mayPromote()
    {
        return mayPromote;
    }

    public void setMayPromote(@Nullable Boolean mayPromoteFlag)
    {
        mayPromote = mayPromoteFlag;
    }

    public Integer getPromotionScore()
    {
        return promotionScore;
    }

    public void setPromotionScore(Integer promotionScoreRef)
    {
        promotionScore = promotionScoreRef;
    }

    @Override
    public Boolean isSuspended()
    {
        return isSuspended;
    }

    @Override
    public void setIsSuspended(boolean isSuspendedRef)
    {
        isSuspended = isSuspendedRef;
    }

    public void setResFileExists(boolean resFileExistsRef)
    {
        resFileExists = resFileExistsRef;
    }

    public boolean resFileExists()
    {
        return resFileExists;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtx) throws AccessDeniedException
    {
        List<DrbdVlmPojo> vlmPojos = new ArrayList<>();
        for (DrbdVlmData<RSC> drbdVlmData : vlmMap.values())
        {
            vlmPojos.add(drbdVlmData.asPojo(accCtx));
        }
        return new DrbdRscPojo(
            rscLayerId,
            getChildrenPojos(accCtx),
            getResourceNameSuffix(),
            drbdRscDfnData.getApiData(accCtx),
            nodeId.get().value,
            peerSlots,
            alStripes,
            alStripeSize,
            flags.getFlagsBits(accCtx),
            vlmPojos,
            suspend.get(),
            promotionScore,
            mayPromote,
            ignoreReason.get()
        );
    }

    @Override
    public int compareTo(DrbdRscData<RSC> other)
    {
        return Integer.compare(rscLayerId, other.rscLayerId);
    }
}
