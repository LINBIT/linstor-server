package com.linbit.linstor.debug;

import com.linbit.linstor.ControllerDebugCmd;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.CtrlDebugControl;
import java.util.Map;

/**
 * Base class for debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseControllerDebugCmd extends BaseDebugCmd implements ControllerDebugCmd
{
    Controller          controller;
    CtrlDebugControl    debugCtl;

    public BaseControllerDebugCmd(
        String[]            cmdNamesRef,
        String              cmdInfoRef,
        String              cmdDescrRef,
        Map<String, String> paramDescrRef,
        String              undeclDescrRef,
        boolean             acceptsUndeclaredFlag
    )
    {
        super(cmdNamesRef, cmdInfoRef, cmdDescrRef, paramDescrRef, undeclDescrRef, acceptsUndeclaredFlag);
        controller  = null;
        debugCtl    = null;
    }

    @Override
    public void initialize(
        Controller      controllerRef,
        CoreServices    coreSvcsRef,
        CtrlDebugControl    debugCtlRef,
        DebugConsole    debugConRef
    )
    {
        commonInitialize(controllerRef, coreSvcsRef, debugCtlRef, debugConRef);
        controller  = controllerRef;
        debugCtl    = debugCtlRef;
    }
}
