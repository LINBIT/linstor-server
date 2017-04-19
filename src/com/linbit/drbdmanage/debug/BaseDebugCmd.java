package com.linbit.drbdmanage.debug;

import com.linbit.AutoIndent;
import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.CommonDebugControl;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.DrbdManage;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Base class for debug console commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseDebugCmd implements CommonDebugCmd
{
    final Set<String> cmdNames;
    final String      cmdInfo;
    final String      cmdDescr;

    final Map<String, String> paramDescr;
    final String      undeclDescr;

    final boolean acceptsUndeclared = false;

    private boolean initialized;

    final Map<String, String> dspNameMap;

    DrbdManage          drbdManage;
    CoreServices        coreSvcs;
    CommonDebugControl  cmnDebugCtl;
    DebugConsole        debugCon;

    public BaseDebugCmd(
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
        coreSvcs    = null;
    }

    @Override
    public void commonInitialize(
        DrbdManage          dmRef,
        CoreServices        coreSvcsRef,
        CommonDebugControl  cmnDebugCtlRef,
        DebugConsole        debugConRef
    )
    {
        drbdManage  = dmRef;
        coreSvcs    = coreSvcsRef;
        cmnDebugCtl = cmnDebugCtlRef;
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

    public void printMissingParamError(
        PrintStream debugErr,
        String paramName
    )
        throws IOException
    {
        printError(
            debugErr,
            String.format(
                "The required parameter '%s' is not present.",
                paramName
            ),
            null,
            "Reenter the command including the required parameter",
            null
        );
    }

    public void printError(
        PrintStream debugErr,
        String errorText,
        String causeText,
        String correctionText,
        String errorDetailsText
    )
        throws IOException
    {
        if (errorText != null)
        {
            debugErr.println("Error:");
            AutoIndent.printWithIndent(debugErr, 4, errorText);
        }
        if (causeText != null)
        {
            debugErr.println("Cause:");
            AutoIndent.printWithIndent(debugErr, 4, causeText);
        }
        if (correctionText != null)
        {
            debugErr.println("Correction:");
            AutoIndent.printWithIndent(debugErr, 4, correctionText);
        }
        if (errorDetailsText != null)
        {
            debugErr.println("Error details:");
            AutoIndent.printWithIndent(debugErr, 4, errorDetailsText);
        }
    }
}
