package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.Controller.DebugControl;
import com.linbit.drbdmanage.CoreServices;

/**
 * Interface for Controller debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ControllerDebugCmd extends CommonDebugCmd
{
    void initialize(
        Controller      controllerRef,
        CoreServices    coreSvcsRef,
        DebugControl    debugCtlRef,
        DebugConsole    debugConRef
    );
}
