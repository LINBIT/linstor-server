package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.core.StltDebugControl;

import java.util.Map;

/**
 * Base class for debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseSatelliteDebugCmd extends BaseDebugCmd implements SatelliteDebugCmd
{
    Satellite       satellite;
    StltDebugControl debugCtl;

    public BaseSatelliteDebugCmd(
        String[]            cmdNamesRef,
        String              cmdInfoRef,
        String              cmdDescrRef,
        Map<String, String> paramDescrRef,
        String              undeclDescrRef,
        boolean             acceptsUndeclaredFlag
    )
    {
        super(cmdNamesRef, cmdInfoRef, cmdDescrRef, paramDescrRef, undeclDescrRef, acceptsUndeclaredFlag);
        satellite   = null;
        debugCtl    = null;
    }

    @Override
    public void initialize(
        Satellite       satelliteRef,
        CoreServices    coreSvcsRef,
        StltDebugControl    debugCtlRef,
        DebugConsole    debugConRef
    )
    {
        commonInitialize(satelliteRef, coreSvcsRef, debugCtlRef, debugConRef);
        satellite   = satelliteRef;
        debugCtl    = debugCtlRef;
    }
}
