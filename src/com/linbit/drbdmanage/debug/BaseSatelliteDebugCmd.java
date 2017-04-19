package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.Satellite;
import com.linbit.drbdmanage.Satellite.DebugControl;
import java.util.Map;

/**
 * Base class for debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseSatelliteDebugCmd extends BaseDebugCmd implements SatelliteDebugCmd
{
    Satellite satellite;
    DebugControl debugCtl;

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
        Satellite       controllerRef,
        CoreServices    coreSvcsRef,
        DebugControl    debugCtlRef,
        DebugConsole    debugConRef
    )
    {
        commonInitialize(controllerRef, coreSvcsRef, debugCtlRef, debugConRef);
        satellite   = controllerRef;
        debugCtl    = debugCtlRef;
    }
}
