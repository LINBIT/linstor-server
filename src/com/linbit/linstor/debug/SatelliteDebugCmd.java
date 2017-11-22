package com.linbit.linstor.debug;

import com.linbit.linstor.CoreServices;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.core.StltDebugControl;

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
