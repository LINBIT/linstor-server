package com.linbit.drbdmanage;

import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.core.CtrlDebugControl;
import com.linbit.drbdmanage.debug.CommonDebugCmd;
import com.linbit.drbdmanage.debug.DebugConsole;

/**
 * Interface for Controller debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ControllerDebugCmd extends CommonDebugCmd
{
    void initialize(
        Controller          controllerRef,
        CoreServices        coreSvcsRef,
        CtrlDebugControl    debugCtlRef,
        DebugConsole        debugConRef
    );
}
