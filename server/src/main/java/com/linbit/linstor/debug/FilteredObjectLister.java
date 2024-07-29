package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class FilteredObjectLister<SearchType>
{
    public static final String PRM_NAME = "NAME";
    public static final String PRM_FILTER_NAME = "MATCHNAME";

    private final String searchTypeName;
    private final String objectTypeName;
    private final ObjectHandler<SearchType> objectHandler;

    private final DebugPrintHelper debugPrintHelper;

    public FilteredObjectLister(
        final String searchTypeNameRef,
        final String objectTypeNameRef,
        final ObjectHandler<SearchType> objectHandlerRef
    )
    {
        searchTypeName = searchTypeNameRef;
        objectTypeName = objectTypeNameRef;
        objectHandler = objectHandlerRef;

        debugPrintHelper = DebugPrintHelper.getInstance();
    }

    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        String prmName = parameters.get(PRM_NAME);
        String prmFilter = parameters.get(PRM_FILTER_NAME);

        try
        {
            if (prmName != null)
            {
                if (prmFilter == null)
                {
                    try (LockGuard scopeLock = LockGuard.createLocked(objectHandler.getRequiredLocks()))
                    {
                        objectHandler.ensureSearchAccess(accCtx);

                        SearchType objectRef = objectHandler.getByName(prmName);
                        if (objectRef != null)
                        {
                            debugPrintHelper.printSectionSeparator(debugOut);
                            objectHandler.displayObjects(debugOut, objectRef, accCtx);
                            debugPrintHelper.printSectionSeparator(debugOut);
                        }
                        else
                        {
                            debugOut.printf("The " + objectTypeName + " '%s' does not exist\n", prmName);
                        }
                    }
                }
                else
                {
                    debugPrintHelper.printError(
                        debugErr,
                        "The command line contains conflicting parameters",
                        "The parameters " + PRM_NAME + " and " + PRM_FILTER_NAME + " were combined " +
                        "in the command line.\n" +
                        "Combining the two parameters is not supported.",
                        "Specify either the " + PRM_NAME + " parameter to display information " +
                        "about a single " + searchTypeName + ", or specify the " + PRM_FILTER_NAME +
                        " parameter to display information about all " + searchTypeName + " entries " +
                        "that have a name matching the specified filter.",
                        null
                    );
                }
            }
            else
            {
                // Filter matching nodes

                int count = 0;
                int total = 0;
                try (LockGuard scopeLock = LockGuard.createLocked(objectHandler.getRequiredLocks()))
                {
                    boolean first = true;

                    Collection<SearchType> objects = objectHandler.getAll();

                    total = objects.size();

                    objectHandler.ensureSearchAccess(accCtx);

                    Matcher nameMatcher = null;
                    if (prmFilter != null)
                    {
                        Pattern namePattern = Pattern.compile(prmFilter, Pattern.CASE_INSENSITIVE);
                        nameMatcher = namePattern.matcher("");
                    }

                    Iterator<SearchType> objectIter = objects.iterator();
                    if (nameMatcher == null)
                    {
                        while (objectIter.hasNext())
                        {
                            SearchType searchObject = objectIter.next();
                            if (first)
                            {
                                debugPrintHelper.printSectionSeparator(debugOut);
                                first = false;
                            }
                            objectHandler.displayObjects(debugOut, searchObject, accCtx);
                            count += objectHandler.countObjects(searchObject, accCtx);
                        }
                    }
                    else
                    {
                        while (objectIter.hasNext())
                        {
                            SearchType searchObject = objectIter.next();
                            String name = objectHandler.getName(searchObject);
                            nameMatcher.reset(name);
                            if (nameMatcher.find())
                            {
                                if (first)
                                {
                                    debugPrintHelper.printSectionSeparator(debugOut);
                                    first = false;
                                }
                                objectHandler.displayObjects(debugOut, searchObject, accCtx);
                                count += objectHandler.countObjects(searchObject, accCtx);
                            }
                        }
                    }
                }

                String totalFormat;
                if (total == 1)
                {
                    totalFormat = "%d " + searchTypeName + " entry is registered in the database\n";
                }
                else
                {
                    totalFormat = "%d " + searchTypeName + " entries are registered in the database\n";
                }

                if (count > 0)
                {
                    debugPrintHelper.printSectionSeparator(debugOut);
                    if (prmFilter == null)
                    {
                        debugOut.printf(totalFormat, total);
                    }
                    else
                    {
                        String countFormat;
                        if (count == 1)
                        {
                            countFormat = "%d " + objectTypeName + " entry was selected by the filter\n";
                        }
                        else
                        {
                            countFormat = "%d " + objectTypeName + " entries were selected by the filter\n";
                        }
                        debugOut.printf(countFormat, count);
                        debugOut.printf(totalFormat, total);
                    }
                }
                else
                {
                    if (total == 0)
                    {
                        debugOut.println("The database contains no " + objectTypeName + " entries");
                    }
                    else
                    {

                        debugOut.println("No matching " + objectTypeName + " entries were found");
                        debugOut.printf(totalFormat, total);
                    }
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            debugPrintHelper.printLsException(debugErr, accExc);
        }
        catch (InvalidNameException nameExc)
        {
            debugPrintHelper.printError(
                debugErr,
                "The value specified for the parameter " + PRM_NAME + " is not a valid " +
                searchTypeName + " name",
                null,
                "Specify a valid " + searchTypeName + " name to display information about a " +
                "single " + searchTypeName + ", or set a filter pattern " +
                "using the " + PRM_FILTER_NAME + " parameter to display information about all " + objectTypeName +
                " entries that have a name that matches the pattern.",
                String.format(
                    "The specified value was '%s'.",
                    prmName
                )
            );
        }
        catch (PatternSyntaxException patternExc)
        {
            debugPrintHelper.printError(
                debugOut,
                "The regular expression specified for the parameter " + PRM_FILTER_NAME + " is not valid.",
                patternExc.getMessage(),
                "Reenter the command using the correct regular expression syntax for the " + PRM_FILTER_NAME +
                " parameter.",
                null
            );
        }
    }

    public interface ObjectHandler<SearchType>
    {
        Lock[] getRequiredLocks();

        void ensureSearchAccess(AccessContext accCtx)
            throws AccessDeniedException;

        Collection<SearchType> getAll();

        @Nullable
        SearchType getByName(String name)
            throws InvalidNameException;

        String getName(SearchType searchObject);

        void displayObjects(PrintStream output, SearchType searchObject, AccessContext accCtx);

        int countObjects(SearchType searchObject, AccessContext accCtx);
    }
}
