package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.Satellite;
import com.linbit.drbdmanage.Satellite.DebugControl;

/**
 * Interface for Satellite debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SatelliteDebugCmd
{
    void initialize(
        Satellite       satelliteRef,
        CoreServices    coreSvcsRef,
        DebugControl    debugCtlRef,
        DebugConsole    debugConRef
    );
}
