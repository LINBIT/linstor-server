package com.linbit.linstor.core;

import com.linbit.linstor.Resource;
import com.linbit.linstor.SatelliteCoreServices;
import com.linbit.linstor.Volume;
import com.linbit.linstor.logging.ErrorReporter;

class DrbdDeviceHandler implements DeviceHandler
{
    private SatelliteCoreServices coreSvcs;
    private ErrorReporter errLog;

    DrbdDeviceHandler(SatelliteCoreServices coreSvcsRef)
    {
        coreSvcs = coreSvcsRef;
        errLog = coreSvcsRef.getErrorReporter();
    }

    /**
     * Entry point for the DeviceManager
     *
     * @param rsc The resource to perform changes on
     */
    @Override
    public void dispatchResource(Resource rsc)
    {
        // TODO: Implement
        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - Begin resource '" +
            rsc.getDefinition().getName().displayValue + "' check simulation (5 seconds)"
        );

        try
        {
            // FIXME: remove this sleep
            // this is only for testing / simulating work
            Thread.sleep(5000);
        }
        catch (InterruptedException exc)
        {
        }

        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - End resource '" +
            rsc.getDefinition().getName().displayValue + "' check simulation"
        );
    }

    /**
     * Ascertains the current status of a resource and determines the changes
     * required to reach the target state that was set for the resource
     */
    void evaluateResource(Resource rsc)
    {

    }

    /**
     * Creates a new resource with empty volumes or with volumes restored from snapshots
     */
    void createResource(Resource rsc)
    {

    }

    /**
     * Restores snapshots into the volumes of a new resource
     */
    void restoreResource(Resource rsc)
    {

    }

    /**
     * Deletes a resource
     * (and will somehow have to inform the device manager, or directly the controller, if the resource
     * was successfully deleted)
     */
    void deleteResource(Resource rsc)
    {

    }

    /**
     * Takes a snapshot from a resource's volumes
     */
    void createSnapshot(Resource rsc)
    {

    }

    /**
     * Deletes a snapshot
     */
    void deleteSnapshot(Resource rsc)
    {

    }

    /**
     * Handles volume creation
     */
    void createVolume(Resource rsc, Volume vlm)
    {

    }

    /**
     * Handles volume deletion
     */
    void deleteVolume(Resource rsc, Volume vlm)
    {

    }

    /**
     * Creates a new empty volume
     */
    void newEmptyVolume(Resource rsc, Volume vlm)
    {

    }

    /**
     * Restores a volume snapshot into a new volume
     */
    void newVolumeFromSnapshot(Resource rsc, Volume vlm)
    {

    }
}
