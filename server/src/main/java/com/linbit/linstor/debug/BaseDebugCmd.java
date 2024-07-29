package com.linbit.linstor.debug;

import com.linbit.ErrorCheck;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

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

    final @Nullable Map<String, String> paramDescr;
    final @Nullable String undeclDescr;

    final boolean acceptsUndeclared = false;

    final DebugPrintHelper debugPrintHelper;

    final Map<String, String> dspNameMap;

    @Nullable DebugConsole debugCon;

    public BaseDebugCmd(
        String[] cmdNamesRef,
        String cmdInfoRef,
        String cmdDescrRef,
        @Nullable Map<String, String> paramDescrRef,
        @Nullable String undeclDescrRef
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

        debugPrintHelper = DebugPrintHelper.getInstance();
    }

    @Override
    public Set<String> getCmdNames()
    {
        Set<String> namesCpy = new TreeSet<>();
        namesCpy.addAll(cmdNames);
        return namesCpy;
    }

    @Override
    public @Nullable String getDisplayName(String upperCaseCmdName)
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
    public @Nullable Map<String, String> getParametersDescription()
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
    public @Nullable String getUndeclaredParametersDescription()
    {
        return undeclDescr;
    }

    @Override
    public boolean acceptsUndeclaredParameters()
    {
        return acceptsUndeclared;
    }

    @Override
    public boolean requiresScope()
    {
        return false;
    }

    public void printMissingParamError(
        PrintStream debugErr,
        String paramName
    )
    {
        debugPrintHelper.printMissingParamError(debugErr, paramName);
    }

    public void printMultiMissingParamError(
        PrintStream debugErr,
        Map<String, String> parameters,
        String... paramNameList
    )
    {
        debugPrintHelper.printMultiMissingParamError(debugErr, parameters, paramNameList);
    }

    public void printLsException(PrintStream debugErr, LinStorException lsExc)
    {
        debugPrintHelper.printLsException(debugErr, lsExc);
    }

    public void printError(
        PrintStream debugErr,
        @Nullable String errorText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String errorDetailsText
    )
    {
        debugPrintHelper.printError(debugErr, errorText, causeText, correctionText, errorDetailsText);
    }

    public void printSectionSeparator(PrintStream output)
    {
        debugPrintHelper.printSectionSeparator(output);
    }
}
