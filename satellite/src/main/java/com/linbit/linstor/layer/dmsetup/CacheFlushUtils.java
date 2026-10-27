package com.linbit.linstor.layer.dmsetup;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import java.util.Set;

/**
 * <p>Decides when a cache layer (dm-cache, dm-writecache) that is not the root layer of a resource has to flush its
 * cache to the origin device, because a storage snapshot is about to be taken below it.</p>
 *
 * <p>The root layer (usually DRBD) is suspended in the first round trip of a snapshot (also of the snapshot a clone
 * takes of its source). Sub-root cache layers are not touched in that round trip: a suspended dm-cache device below
 * DRBD blocks DRBD's own writes (metadata, replicated writes a peer still waits for), which can stall the suspend of
 * the other nodes. Instead the cache layers flush in the second round trip, directly around the storage snapshot, when
 * processing the snapshot.</p>
 *
 * <p>This is only done while the root layer is still suspended, which limits it to the second round trip: the third
 * round trip resumes the root layer before the snapshots are processed again.</p>
 */
public final class CacheFlushUtils
{
    /**
     * The processing of a layer's children that has to happen while the cache is flushed, i.e. a call to
     * {@link com.linbit.linstor.core.devmgr.DeviceHandler#processSnapshot}.
     */
    @FunctionalInterface
    public interface ChildProcessing
    {
        void process() throws StorageException, ResourceException, VolumeException, DatabaseException;
    }

    private CacheFlushUtils()
    {
    }

    /**
     * Returns the resource layer data of the live resource that has to be flushed before the storage snapshot
     * below the given snapshot layer data is taken, or {@code null} if no flush is needed.
     *
     * @param snapDataRef the cache layer data of the snapshot
     */
    public static @Nullable AbsRscLayerObject<Resource> getRscDataToFlushForSnapshot(
        AbsRscLayerObject<Snapshot> snapDataRef
    )
    {
        @Nullable AbsRscLayerObject<Resource> ret = null;
        Snapshot snap = snapDataRef.getAbsResource();
        boolean snapshotToBeTaken = snap.getTakeSnapshot() && snap.getFlags().isUnset(Snapshot.Flags.DELETE);
        // a root cache layer was already suspended (including its flush) in the first round trip
        if (snapshotToBeTaken && snapDataRef.getParent() != null)
        {
            @Nullable Resource rsc = snap.getResourceDefinition().getResource(snap.getNodeName());
            if (rsc != null)
            {
                Set<AbsRscLayerObject<Resource>> candidates = LayerRscUtils.getRscDataByLayer(
                    rsc.getLayerData(),
                    snapDataRef.getLayerKind(),
                    snapDataRef.getResourceNameSuffix()::equals
                );
                if (candidates.size() > 1)
                {
                    throw new ImplementationError(
                        "Unexpected number of " + snapDataRef.getLayerKind() + " layer data for: " +
                            snapDataRef.getSuffixedResourceName()
                    );
                }
                // no candidate: the resource has no matching cache layer data, so there is nothing to flush
                if (!candidates.isEmpty())
                {
                    AbsRscLayerObject<Resource> rscData = candidates.iterator().next();
                    if (isInSuspendWindow(rscData))
                    {
                        ret = rscData;
                    }
                }
            }
        }
        return ret;
    }

    /**
     * The given layer data exists, its resource is active and the resource's root layer is (to be) suspended.
     */
    private static boolean isInSuspendWindow(AbsRscLayerObject<Resource> rscDataRef)
    {
        AbsRscLayerObject<Resource> root = rscDataRef;
        while (root.getParent() != null)
        {
            root = root.getParent();
        }
        boolean isRscInactive = rscDataRef.getAbsResource().getStateFlags().isSomeSet(
            Resource.Flags.INACTIVE,
            Resource.Flags.INACTIVE_PERMANENTLY,
            Resource.Flags.INACTIVATING
        );
        return rscDataRef.exists() && !isRscInactive && root.getShouldSuspendIo();
    }
}
