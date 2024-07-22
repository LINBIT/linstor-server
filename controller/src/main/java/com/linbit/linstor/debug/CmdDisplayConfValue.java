package com.linbit.linstor.debug;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class CmdDisplayConfValue extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_KEY = "KEY";
    private static final String PRM_FILTER_KEY = "MATCHKEY";
    private static final String PRM_FILTER_VALUE = "MATCHVALUE";
    private static final String PRM_NAMESPACE = "NAMESPACE";

    private static final String ENTRY_HEADER_FORMAT = "\u001b[1;37m%-40s = %s\u001b[0m\n";
    private static final String ENTRY_OUTPUT_FORMAT = "%-40s = %s\n";
    private static final String HEADER_KEY = "Key";
    private static final String HEADER_VALUE = "Value";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_KEY,
            "Key of the configuration property to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_KEY,
            "Filter pattern to apply to the configuration property's key.\n" +
            "Entries with a key matching the pattern will be displayed."
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_VALUE,
            "Filter pattern to apply to the configuration property's value.\n" +
            "Entries with a value matching the pattern will be displayed."
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_NAMESPACE,
            "Namespace (path) in the configuration.\n" +
            "Entries from the selected namespace will be listed, unless a key specifies\n" +
            "an absolute path outside of the namespace."
        );
    }

    private final ReadWriteLock confLock;
    private final SystemConfRepository systemConfRepository;

    @Inject
    public CmdDisplayConfValue(
        @Named(CoreModule.CTRL_CONF_LOCK) ReadWriteLock confLockRef,
        SystemConfRepository systemConfRepositoryRef
    )
    {
        super(
            new String[]
            {
                "DspCfgVal"
            },
            "Display configuration value(s)",
            "Displays values associated with the selected configuration keys",
            PARAMETER_DESCRIPTIONS,
            null
        );

        confLock = confLockRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        String prmKey = parameters.get(PRM_KEY);
        String prmFilterKey = parameters.get(PRM_FILTER_KEY);
        String prmFilterValue = parameters.get(PRM_FILTER_VALUE);
        String prmNamespace = parameters.get(PRM_NAMESPACE);

        // Allow either specifying the key, or specifying one or both of the filters,
        // or specifying nothing, but not the combination of specifying the key and any of the filters
        if (prmKey == null || (prmFilterKey == null && prmFilterValue == null))
        {
            confLock.readLock().lock();
            try
            {
                @Nullable ReadOnlyProps searchRoot = systemConfRepository.getCtrlConfForView(accCtx);
                if (prmNamespace != null)
                {
                    searchRoot = searchRoot.getNamespace(prmNamespace);
                    if (searchRoot == null)
                    {
                        throw new NamespaceException(
                            String.format(
                                "The specified namespace '%s' does not exist", prmNamespace
                            )
                        );
                    }
                }
                if (prmKey == null && prmFilterKey == null && prmFilterValue == null)
                {
                    long count = 0;
                    for (Map.Entry<String, String> confEntry : searchRoot)
                    {
                        if (count == 0)
                        {
                            debugOut.printf(ENTRY_HEADER_FORMAT, HEADER_KEY, HEADER_VALUE);
                            printSectionSeparator(debugOut);
                        }
                        debugOut.printf(ENTRY_OUTPUT_FORMAT, confEntry.getKey(), confEntry.getValue());
                        ++count;
                    }
                    if (count > 0)
                    {
                        printSectionSeparator(debugOut);
                        if (count == 1)
                        {
                            debugOut.println("1 entry");
                        }
                        else
                        {
                            debugOut.printf("%d entries\n", count);
                        }
                    }
                    else
                    {
                        debugOut.printf(
                            "Configuration namespace %s does not contain any entries\n",
                            searchRoot.getPath()
                        );
                    }
                }
                else
                if (prmKey != null)
                {
                    String value = searchRoot.getProp(prmKey);
                    if (value == null)
                    {
                        debugOut.printf("Key '%s' does not exist in the configuration.\n", prmKey);
                    }
                    else
                    {
                        debugOut.printf(ENTRY_HEADER_FORMAT, HEADER_KEY, HEADER_VALUE);
                        printSectionSeparator(debugOut);
                        debugOut.printf(ENTRY_OUTPUT_FORMAT, prmKey, value);
                        printSectionSeparator(debugOut);
                    }
                }
                else
                {
                   Matcher keyMatcher = null;
                   Matcher valueMatcher = null;

                   if (prmFilterKey != null)
                   {
                       try
                       {
                           Pattern keyPattern = Pattern.compile(prmFilterKey, Pattern.CASE_INSENSITIVE);
                           keyMatcher = keyPattern.matcher("");
                       }
                       catch (PatternSyntaxException patternExc)
                       {
                           printPatternError(debugErr, PRM_FILTER_KEY, patternExc.getMessage());
                           throw patternExc;
                       }
                   }

                   if (prmFilterValue != null)
                   {
                       try
                       {
                           Pattern valuePattern = Pattern.compile(prmFilterValue, Pattern.CASE_INSENSITIVE);
                           valueMatcher = valuePattern.matcher("");
                       }
                       catch (PatternSyntaxException patternExc)
                       {
                           printPatternError(debugErr, PRM_FILTER_VALUE, patternExc.getMessage());
                           throw patternExc;
                       }
                   }

                   long count = 0;
                   for (Map.Entry<String, String> confEntry : searchRoot)
                   {
                       String key = confEntry.getKey();
                       String value = confEntry.getValue();

                       boolean selected = true;
                       if (keyMatcher != null)
                       {
                           keyMatcher.reset(key);
                           selected &= keyMatcher.find();
                       }
                       if (valueMatcher != null)
                       {
                           valueMatcher.reset(value);
                           selected &= valueMatcher.find();
                       }

                       if (selected)
                       {
                           if (count == 0)
                           {
                               debugOut.printf(ENTRY_HEADER_FORMAT, HEADER_KEY, HEADER_VALUE);
                               printSectionSeparator(debugOut);
                           }
                           debugOut.printf(ENTRY_OUTPUT_FORMAT, confEntry.getKey(), confEntry.getValue());
                           ++count;
                       }
                   }
                   if (count > 0)
                   {
                       printSectionSeparator(debugOut);
                       if (count == 1)
                       {
                           debugOut.println("1 entry matches");
                       }
                       else
                       {
                           debugOut.printf("%d entries match\n", count);
                       }
                   }
                   else
                   {
                       debugOut.println("No matching entries were found");
                   }
                }
            }
            catch (PatternSyntaxException patternExc)
            {
                // Already handled
            }
            catch (NamespaceException nameSpcExc)
            {
                debugOut.println(nameSpcExc.getMessage());
            }
            catch (LinStorException lsExc)
            {
                printLsException(debugErr, lsExc);
            }
            finally
            {
                confLock.readLock().unlock();
            }
        }
        else
        {
            String causeText = null;
            if (prmFilterKey != null && prmFilterValue != null)
            {
                causeText = String.format(
                    "The %s parameter was specified in combination with the %s and %s parameters",
                    PRM_KEY, PRM_FILTER_KEY, PRM_FILTER_VALUE
                );
            }
            else
            if (prmFilterKey != null)
            {
                causeText = String.format(
                    "The %s parameter was specified in combination with the %s parameters",
                    PRM_KEY, PRM_FILTER_KEY
                );
            }
            else
            if (prmFilterValue != null)
            {
                causeText = String.format(
                    "The %s parameter was specified in combination with the %s parameters",
                    PRM_KEY, PRM_FILTER_VALUE
                );
            }
            else
            {
                throw new ImplementationError(
                    "Logic error encountered while handling conflicting command parameters",
                    null
                );
            }
            printError(
                debugErr,
                "Conflicting parameters were entered for the command",
                causeText,
                String.format(
                    "Specify either the %s parameter, or one or both of the %s and %s parameters.",
                    PRM_KEY, PRM_FILTER_KEY, PRM_FILTER_VALUE
                ),
                null
            );
        }
    }

    private void printPatternError(
        PrintStream output,
        String filterParam,
        String patternExcMsg
    )
    {
        printError(
            output,
            String.format(
                "The %s parameter specifies an invalid regular expression pattern.",
                filterParam
            ),
            patternExcMsg,
            "Reenter the command using the correct regular expression syntax for the filter parameter.",
            null
        );
    }

    private static class NamespaceException extends Exception
    {
        NamespaceException(String message)
        {
            super(message);
        }
    }
}
