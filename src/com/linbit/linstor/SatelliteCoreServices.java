package com.linbit.linstor;

import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.drbdstate.DrbdStateTracker;

/**
 * Core services of the linstor Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SatelliteCoreServices extends CoreServices
{
    FileSystemWatch getFsWatch();
    DeviceManager getDeviceManager();
    DrbdStateTracker getDrbdStateTracker();
}
