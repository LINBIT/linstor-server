package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

/**
 * What {@link com.linbit.linstor.core.devmgr.DeviceHandler#openSnapshotReadOnly} opens, and for whom:
 * <ul>
 *  <li>{@link ForSnapshot}: the LINSTOR snapshot of snapshot volume data</li>
 *  <li>{@link ForClone}: the clone snapshot of resource volume data (the clone source)</li>
 * </ul>
 * The volume data is part of the opener and typed accordingly, so that snapshot volume data cannot be combined with
 * a clone and vice versa. Layers pass the opener on to the layer below with {@link #withVlmData}.
 *
 * @param <RSC> {@link Snapshot} or {@link Resource}, depending on the opener
 */
public sealed interface ReadOnlyOpener<RSC extends AbsResource<RSC>>
{
    /**
     * The volume data of the layer that is currently opened.
     */
    VlmProviderObject<RSC> vlmData();

    /**
     * Returns the same opener for the given volume data of the layer below.
     */
    ReadOnlyOpener<RSC> withVlmData(VlmProviderObject<RSC> childVlmData);

    /**
     * Appended to the names of the device mappings created while opening (e.g. the dm-crypt mapping of a LUKS
     * layer), so that they differ from the mappings of the live resource, of other snapshots of the same resource
     * and of other openers.
     */
    String mappingSuffix();

    /**
     * Opens the clone snapshot that was taken of the given source resource volume for the given clone target.
     */
    static ForClone forClone(VlmProviderObject<Resource> srcVlmDataRef, String cloneTargetNameRef)
    {
        return new ForClone(srcVlmDataRef, cloneTargetNameRef);
    }

    /**
     * Opens the given LINSTOR snapshot volume. The suffix has to be unique among the openers of the same snapshot
     * volume, see {@link ForSnapshot}.
     */
    static ForSnapshot forSnapshot(VlmProviderObject<Snapshot> snapVlmDataRef, String openerSuffixRef)
    {
        return new ForSnapshot(snapVlmDataRef, openerSuffixRef);
    }

    /**
     * Opens the clone snapshot of a resource volume (the clone source).
     *
     * @param cloneTargetName the name of the clone target, from which the storage layer derives the name of the
     *     clone snapshot
     */
    record ForClone(VlmProviderObject<Resource> vlmData, String cloneTargetName) implements ReadOnlyOpener<Resource>
    {
        @Override
        public ForClone withVlmData(VlmProviderObject<Resource> childVlmData)
        {
            return new ForClone(childVlmData, cloneTargetName);
        }

        @Override
        public String mappingSuffix()
        {
            return InternalApiConsts.CLONE_FOR_PREFIX + cloneTargetName;
        }
    }

    /**
     * Opens the LINSTOR snapshot of a snapshot volume. The device mappings get the snapshot name plus the suffix of
     * the opener appended, so that several snapshots of the same resource can be open at the same time.
     *
     * @param openerSuffix identifies the opener, see {@link #MAX_OPENER_SUFFIX_LENGTH}
     */
    record ForSnapshot(VlmProviderObject<Snapshot> vlmData, String openerSuffix) implements ReadOnlyOpener<Snapshot>
    {
        /**
         * Keeps device mapping names within the device mapper's limit of 127 characters, also for maximum length
         * resource and snapshot names (e.g. {@code Linstor-Crypt-<rsc>_<vlmNr>_<snapshot><openerSuffix>}).
         */
        public static final int MAX_OPENER_SUFFIX_LENGTH = 8;

        public ForSnapshot
        {
            if (openerSuffix.isEmpty() || openerSuffix.length() > MAX_OPENER_SUFFIX_LENGTH)
            {
                throw new ImplementationError(
                    "The opener suffix must have 1 to " + MAX_OPENER_SUFFIX_LENGTH + " characters, but was '" +
                        openerSuffix + "'"
                );
            }
        }

        @Override
        public ForSnapshot withVlmData(VlmProviderObject<Snapshot> childVlmData)
        {
            return new ForSnapshot(childVlmData, openerSuffix);
        }

        @Override
        public String mappingSuffix()
        {
            return "_" + vlmData.getRscLayerObject().getAbsResource().getSnapshotName().displayValue + openerSuffix;
        }
    }
}
