package com.linbit.linstor.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.numberpool.BitmapPool;
import com.linbit.linstor.numberpool.NumberPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class's purpose is to shorten the resource name uniquely but also trying to stay
 * as close to the original as possible.
 *
 * This is done by replacing as many characters as needed at the end of the resulting string
 * with an incremental number.
 * Example: maxLen = 5, "resourceNameTooLong" will result in "res_1"
 * The 10th resource starting with "res.*" will increase the digit-count, resulting in "re_10"
 */
// purposely not singleton and not injectable
public class NameShortener
{
    public static final String OPENFLEX = "OpenFlex";
    public static final String EXOS = "Exos";

    /**
     * The property namespace used for lookup of already shortened name
     */
    private final String propNamespace;
    /**
     * The property key used for lookup of already shortened name
     */
    private final String propKey;
    /**
     * Maximal length of shortened (result) String
     */
    private final int maxLen;
    /**
     * Delimiter used between baseString and the incremental number
     */
    private final String delimiter;
    private final int delimiterLen;

    private final AccessContext accCtx;

    /**
     * Map of <BaseString, digitCount> to specific numberPools
     */
    private final Map<String, NumberPool> namePools;
    /**
     * Set will contain shortened as well as rscNames that did not have to
     * be shortened. Used to detect any conflicting resource names
     */
    private final Set<String> existingNames;

    /**
     * delimiter + "([1-9]\\d{0,5})$"
     */
    private final Pattern numberPattern;

    /**
     * "[^" + validCharacters (from constructor parameter) + "]"
     * Used to replace everything that matches this regex with empty string.
     * If null, the replacement is skipped
     */
    private final String invalidCharRegex;

    public NameShortener(
        String propNamespaceRef,
        String propKeyRef,
        int maxLenRef,
        AccessContext accCtxRef,
        String delimiterRef,
        String validCharacters
    )
    {
        propNamespace = propNamespaceRef;
        propKey = propKeyRef;
        maxLen = maxLenRef;
        accCtx = accCtxRef;
        delimiter = delimiterRef;
        delimiterLen = delimiterRef.length();

        existingNames = new TreeSet<>();
        namePools = new TreeMap<>();
        numberPattern = Pattern.compile(delimiterRef + "([1-9]\\d{0,5})$");

        invalidCharRegex = validCharacters == null ? null : "[^" + validCharacters + "]";
    }

    public String shorten(ResourceDefinition rscDfn, String rscSuffix)
        throws AccessDeniedException, DatabaseException, LinStorException
    {
        return shorten(rscDfn.getProps(accCtx), "", rscDfn.getName().displayValue + rscSuffix);
    }

    public String shorten(VolumeDefinition vlmDfn, String keyPrefix, String rscSuffix)
        throws AccessDeniedException, DatabaseException, LinStorException
    {
        return shorten(
            vlmDfn.getProps(accCtx),
            keyPrefix,
            vlmDfn.getResourceDefinition().getName().displayValue + rscSuffix +
                String.format("_%05d", vlmDfn.getVolumeNumber().value)
        );
    }

    public String shorten(Props props, String propKeyPrefix, String fullName)
        throws AccessDeniedException, DatabaseException, LinStorException
    {
        String shortName = null;
        try
        {
            String localPropKey = propNamespace + "/" + propKeyPrefix + propKey;
            String ret = props.getProp(localPropKey);
            if (ret == null)
            {
                // rscDfn does not have a property with the shortendName yet

                ret = fullName;
                if (invalidCharRegex != null)
                {
                    ret = ret.replaceAll(invalidCharRegex, "");
                }

                final boolean isExistingName = existingNames.contains(ret);
                if (ret.length() > maxLen || isExistingName)
                {
                    // Name is too long or conflicts with an existing name
                    final NumberPool nrPool = getNumberPool(ret, true);
                    int resourceNr = nrPool.autoAllocate(0, 999_999);
                    String baseName = getBaseName(ret);
                    shortName = baseName + delimiter + resourceNr;

                    // Register the shortened name
                    existingNames.add(shortName);
                    // Set shortName property
                    props.setProp(localPropKey, shortName);
                }
                else
                {
                    // Full resource name is short enough and does not conflict with existing names

                    // Register the name as it is
                    existingNames.add(ret);
                    // Set shortName property
                    props.setProp(localPropKey, shortName);

                    shortName = ret;

                    processNumberSuffix(shortName);
                }
            }
            else
            {
                // property is already set. mostly noop here
                shortName = ret;

                // Register the name in case it was set in a previous controller session
                existingNames.add(shortName);

                processNumberSuffix(shortName);
            }
        }
        catch (ExhaustedPoolException exc)
        {
            throw new LinStorException(
                "Short name generation for resource " + fullName + " failed: " +
                "The number pool for this name space is exhausted"
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }

        return shortName;
    }

    private int parseNumber(String nrText)
    {
        final int number;
        try
        {
            number = Integer.parseInt(nrText);
        }
        catch (NumberFormatException exc)
        {
            throw new ImplementationError(
                "The regular expression \"" + numberPattern.toString() + "\" matched an unparsable character sequence",
                exc
            );
        }
        return number;
    }

    private void processNumberSuffix(String name)
    {
        // If the name ends with a text that conflicts with the numbering pattern generated
        // by name shortening, allocate the number parsed from that pattern to avoid generating
        // a conflicting name (with the same number, resulting in the same text)
        Matcher nrMatcher = numberPattern.matcher(name);
        if (nrMatcher.find())
        {
            final String nrText = nrMatcher.group(1);
            final int resourceNr = parseNumber(nrText);

            final NumberPool nrPool = getNumberPool(name, true);
            nrPool.allocate(resourceNr);
        }
    }

    /**
     * Returns the baseName of a given fullName.
     * Example: fullName="resourceTooLong", maxLen=5, delimiter="_", digitCount=1
     * Results: baseName="res".
     *
     * @param fullName
     * @param digitCount
     *
     * @return
     */
    private String getBaseName(String fullName)
    {
        int splitIdx = fullName.lastIndexOf(delimiter);
        splitIdx += delimiter.length();
        return fullName.substring(splitIdx, fullName.length());
    }

    /**
     * Returns the NumberPool for the Pair of <baseName, digitCount> with lazy initialization.
     * The baseName is calculated from the given parameters.
     *
     * @param fullName
     * @param digitCount
     *
     * @return
     */
    private NumberPool getNumberPool(String fullName, boolean createFlag)
    {
        final String baseName = getBaseName(fullName);
        NumberPool numberPool = namePools.get(baseName);
        if (numberPool == null && createFlag)
        {
            numberPool = new BitmapPool(1_000_000);
            namePools.put(baseName, numberPool);
        }
        return numberPool;
    }

    private void removeNumberPool(String fullName)
    {
        String baseName = getBaseName(fullName);
        namePools.remove(baseName);
    }

    /**
     * Deallocates the reserved number of the shortened name (if any)
     * and forgets about the given name so that it can be chosen again later.
     *
     * @param rscDfn
     * @param rscSuffix
     *
     * @throws AccessDeniedException
     */
    public void remove(ResourceDefinition rscDfn, String rscSuffix) throws AccessDeniedException
    {
        Props rscDfnProps = rscDfn.getProps(accCtx);
        String shortName = rscDfnProps.getProp(propKey);
        if (shortName != null)
        {
            Matcher nrMatcher = numberPattern.matcher(shortName);
            if (nrMatcher.find())
            {
                final int resourceNr = parseNumber(nrMatcher.group(1));
                final NumberPool nrPool = getNumberPool(shortName, false);
                if (nrPool != null)
                {
                    nrPool.deallocate(resourceNr);
                    if (nrPool.isEmpty())
                    {
                        removeNumberPool(shortName);
                    }
                }
            }
            existingNames.remove(shortName);
        }
    }
}
