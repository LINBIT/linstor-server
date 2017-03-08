package com.linbit.drbdmanage.debug;

import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.Controller.DebugConsole;
import com.linbit.drbdmanage.Controller.DebugControl;
import com.linbit.drbdmanage.CoreServices;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Base class for debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseControllerDebugCmd implements ControllerDebugCmd
{
    final Set<String> cmdNames;
    final String      cmdInfo;
    final String      cmdDescr;

    final Map<String, String> paramDescr;
    final String      undeclDescr;

    final boolean acceptsUndeclared = false;

    private boolean initialized;

    final Map<String, String> dspNameMap;

    Controller      controller;
    CoreServices    coreSvcs;
    DebugControl    debugCtl;
    DebugConsole    debugCon;

    public BaseControllerDebugCmd(
        String[]            cmdNamesRef,
        String              cmdInfoRef,
        String              cmdDescrRef,
        Map<String, String> paramDescrRef,
        String              undeclDescrRef,
        boolean             acceptsUndeclaredFlag
    )
    {
        ErrorCheck.ctorNotNull(this.getClass(), String[].class, cmdNamesRef);
        cmdNames    = new TreeSet<>();
        for (String name : cmdNamesRef)
        {
            ErrorCheck.ctorNotNull(this.getClass(), String.class, name);
            cmdNames.add(name);
        }
        dspNameMap  = new TreeMap<>();
        for (String name : cmdNames)
        {
            dspNameMap.put(name.toUpperCase(), name);
        }
        cmdInfo     = cmdInfoRef;
        cmdDescr    = cmdDescrRef;
        paramDescr  = paramDescrRef;
        undeclDescr = undeclDescrRef;
        initialized = false;
        controller  = null;
        coreSvcs    = null;
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
        controller  = controllerRef;
        coreSvcs    = coreSvcsRef;
        debugCtl    = debugCtlRef;
        debugCon    = debugConRef;
        initialized = true;
    }

    @Override
    public Set<String> getCmdNames()
    {
        Set<String> namesCpy = new TreeSet<>();
        namesCpy.addAll(cmdNames);
        return namesCpy;
    }

    @Override
    public String getDisplayName(String upperCaseCmdName)
    {
        return dspNameMap.get(upperCaseCmdName);
    }

    @Override
    public String getCmdInfo()
    {
        return cmdInfo;
    }

    @Override
    public String getCmdDescription()
    {
        return cmdDescr;
    }

    @Override
    public Map<String, String> getParametersDescription()
    {
        Map<String, String> paramCopy = null;
        if (paramDescr != null)
        {
            // Copy the map to prevent modification of the original map
            paramCopy = new TreeMap<>();
            for (Map.Entry<String, String> paramEntry : paramDescr.entrySet())
            {
                paramCopy.put(paramEntry.getKey(), paramEntry.getValue());
            }
        }
        return paramCopy;
    }

    @Override
    public String getUndeclaredParametersDescription()
    {
        return undeclDescr;
    }

    @Override
    public boolean acceptsUndeclaredParameters()
    {
        return acceptsUndeclared;
    }
}
