package com.linbit.linstor;

import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.CtrlDebugControl;
import com.linbit.linstor.debug.CommonDebugCmd;
import com.linbit.linstor.debug.DebugConsole;

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
