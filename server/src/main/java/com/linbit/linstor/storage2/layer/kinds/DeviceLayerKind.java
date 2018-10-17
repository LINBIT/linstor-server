package com.linbit.linstor.storage2.layer.kinds;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume;

import java.util.Collections;
import java.util.Set;

public interface DeviceLayerKind
{
    enum StartupVerifications
    {
        UNAME,
        DRBD9, DRBD_PROXY,
        CRYPT_SETUP
    }

    /**
     * @return true if and only if the associated {@link DeviceLayer} implementation supports
     * snapshots.
     */
    boolean isSnapshotSupported();

    /**
     *
     * @return true if and only if the current {@link DeviceLayer} implementation supports resizing
     * {@link Volume}s.
     */
    boolean isResizeSupported();

    /**
     * @return an array of Verifications the satellite has to perform on startup in order for this
     * DeviceModule to work properly
     */
    StartupVerifications[] requiredVerifications();

    /**
     * Some Device Layers are still interested in updates of a {@link ResourceDefinition} although
     * they do not have a deployed {@link Resource} from the given <tt>ResourceDefinition</tt> (e.g.
     * {@link DrbdProxyLayerKind}).
     * @param rscDfn
     * @return
     */
    default Set<Node> getSpecialInterestedNodes(ResourceDefinition rscDfn)
    {
        return Collections.emptySet();
    }
}
