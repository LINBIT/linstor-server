package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.types.MinorNumber;

/**
 * Observes the state of a DRBD resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceObserver
{
    /**
     * Called when a DRBD resource has been created
     *
     * @param resource DrbdResource instance tracking the new DRBD resource's state
     */
    default void resourceCreated(@Nullable DrbdResource resource)
    {
        // Do nothing
    }

    /**
     * Called when the role of a DRBD resource has changed
     *
     * @param resource Representation of the DRBD resource affected by the event
     * @param previous Role of the resource before the change
     * @param current Role of the resource after the change
     */
    default void roleChanged(
        @Nullable DrbdResource resource,
        DrbdResource.Role previous, DrbdResource.Role current
    )
    {
        // Do nothing
    }

    default void promotionScoreChanged(
        DrbdResource resource,
        @Nullable Integer prevPromitionScore, @Nullable Integer current
    )
    {
    }

    default void mayPromoteChanged(
        DrbdResource resource,
        @Nullable Boolean prevMayPromote, @Nullable Boolean current
    )
    {
    }

    /**
     * Called when the role of a DRBD resource's peer resource (connection) has changed
     *
     * @param resource Representation of the DRBD resource that owns the connection to
     *     the peer resource that is affected by the event
     * @param connection Representation of the connection to the peer resource
     * @param previous Role of the peer resource (connection) before the change
     * @param current Role of the peer resource (connection) after the change
     */
    default void peerRoleChanged(
        DrbdResource resource, DrbdConnection connection,
        DrbdResource.Role previous, DrbdResource.Role current
    )
    {
        // Do nothing
    }

    /**
     * Called when a DRBD resource has been destroyed and is no longer known
     * to the DRBD kernel module
     *
     * @param resource DrbdResource instance tracking the DRBD resource's state. This instance
     *     is no longer known to the DRBD tracking logic when the method is called. The main
     *     purpose of passing it to this method is to inform the observer about the resource's
     *     name and last known state before the resource disappeared.
     */
    default void resourceDestroyed(DrbdResource resource)
    {
        // Do nothing
    }

    /**
     * Called when a DRBD volume or peer volume has been created
     *
     * @param resource Representation of the DRBD resource that owns the volume
     * @param connection If the volume is a peer volume, representation of the
     *     DRBD connection that owns the peer volume; if the volume is a
     *     local volume, this argument is set to null
     * @param volume DrbdVolume instance tracking the new DRBD volume's state
     */
    default void volumeCreated(DrbdResource resource, @Nullable DrbdConnection connection, DrbdVolume volume)
    {
        // Do nothing
    }

    /**
     * Called when the minor number of a local volume has changed.
     * The minor numbers of peer resource's volumes are not tracked.
     *
     * @param resource Representation of the DRBD resource that owns
     *     the volume affected by the event
     * @param volume Representation of the volume affected by the event
     * @param previous Minor number of the volume before the change
     * @param current Minor number of the volume after the change
     */
    default void minorNrChanged(
        @Nullable DrbdResource resource,
        @Nullable DrbdVolume volume,
        MinorNumber previous, MinorNumber current
    )
    {
        // Do nothing
    }

    /**
     * Called when the disk state of a DRBD volume or peer volume has changed
     *
     * @param resource Representation of the DRBD resource that owns
     *     the volume affected by the event
     * @param connection If the volume is a peer volume, representation of the
     *     DRBD connection that owns the peer volume; if the volume is a
     *     local volume, this argument is set to null
     * @param volume Representation of the volume affected by the event
     * @param previous Disk state of the volume before the change
     * @param current Disk state of the volume after the change
     */
    default void diskStateChanged(
        DrbdResource resource,
        @Nullable DrbdConnection connection,
        DrbdVolume volume,
        DiskState previous, DiskState current
    )
    {
        // Do nothing
    }

    /**
     * Called when the replication state of a DRBD volume or
     * peer volume has changed
     *
     * @param resource Representation of the DRBD resource that owns
     *     the volume affected by the event
     * @param connection If the volume is a peer volume, representation of the
     *     DRBD connection that owns the peer volume; if the volume is a
     *     local volume, this argument is set to null
     * @param volume Representation of the volume affected by the event
     * @param previous Replication state of the volume before the change
     * @param current Replication state of the volume after the change
     */
    default void replicationStateChanged(
        DrbdResource resource, DrbdConnection connection, DrbdVolume volume,
        ReplState previous, ReplState current
    )
    {
        // Do nothing
    }

    /**
     * Called when the done percentage changes
     *
     * @param resource Representation of the DRBD resource that owns
     *     the volume affected by the event
     * @param connection If the volume is a peer volume, representation of the
     *     DRBD connection that owns the peer volume; if the volume is a
     *     local volume, this argument is set to null
     * @param volume Representation of the volume affected by the event
     * @param prevPercentage previous done percentage
     * @param current Current done percentage
     */
    default void donePercentageChanged(
        DrbdResource resource, DrbdConnection connection, DrbdVolume volume,
        Float prevPercentage,
        Float current
    )
    {
        // Do nothing
    }

    /**
     * Called when a DRBD resource's volume or peer volume has been destroyed
     * and is no longer known to the DRBD kernel module
     *  @param resource Representation of the DRBD resource that owned the volume
     *     before it disappeared
     * @param connection
     * @param volume DrbdVolume instance tracking the DRBD volume's state. This instance
     *     is no longer known to the DRBD tracking logic when the method is called. The main
     *     purpose of passing it to this method is to inform the observer about the volume's
     */
    default void volumeDestroyed(
        DrbdResource resource,
        @Nullable DrbdConnection connection,
        DrbdVolume volume
    )
    {
        // Do nothing
    }

    /**
     * Called when a DRBD connection has been created
     *
     * @param resource Representation of the DRBD resource that owns the connection
     * @param connection DrbdConnection instance tracking the DRBD connection's state
     */
    default void connectionCreated(DrbdResource resource, DrbdConnection connection)
    {
        // Do nothing
    }

    /**
     * Called when the connection state of a DRBD connection has changed
     *
     * @param resource Representation of the DRBD resource that owns the connection
     * @param connection Representation of the DRBD connection affected by the change
     * @param previous Connection state before the change
     * @param current Connection state after the change
     */
    default void connectionStateChanged(
        DrbdResource resource, DrbdConnection connection,
        DrbdConnection.State previous, DrbdConnection.State current
    )
    {
        // Do nothing
    }

    /**
     * Called when a DRBD connection has been destroyed and is no
     * longer known to the DRBD kernel module
     *
     * @param resource Representation of the DRBD resource that owned the connection
     *     before it disappeared
     * @param connection DrbdConnection instance tracking the DRBD connection's
     *     &amp; peer resource's state. This instance is no longer known to the DRBD
     *     tracking logic when the method is called. The main purpose of passing it
     *     to this method is to inform the observer about the connection's and
     *     peer resource's last known state before the connection disappeared
     */
    default void connectionDestroyed(DrbdResource resource, DrbdConnection connection)
    {
        // Do nothing
    }
}
