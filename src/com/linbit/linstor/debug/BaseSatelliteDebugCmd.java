package com.linbit.linstor.debug;

import java.util.Map;

import com.linbit.linstor.CoreServices;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.core.StltDebugControl;

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
