package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.core.StltDebugControl;

/**
 * Interface for Satellite debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SatelliteDebugCmd extends CommonDebugCmd
{
    void initialize(
        Satellite       satelliteRef,
        CoreServices    coreSvcsRef,
        StltDebugControl    debugCtlRef,
        DebugConsole    debugConRef
    );
}
