package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.Controller.DebugControl;
import com.linbit.drbdmanage.CoreServices;

import java.util.*;

/**
 * Base class for debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseControllerDebugCmd extends BaseDebugCmd implements ControllerDebugCmd
{
    Controller      controller;
    DebugControl    debugCtl;

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
        DebugControl    debugCtlRef,
        DebugConsole    debugConRef
    )
    {
        commonInitialize(controllerRef, coreSvcsRef, debugCtlRef, debugConRef);
        controller  = controllerRef;
        debugCtl    = debugCtlRef;
    }
}
