package com.linbit.linstor.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.numberpool.BitmapPool;
import com.linbit.linstor.numberpool.NumberPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class's purpose is to shorten the resource name uniquely but also trying to stay
 * as close to the original as possible.
 */
// purposely not singleton and not injectable
public class NameShortener
{
    public static final String OPENFLEX = "OpenFlex";
    @Deprecated(forRemoval = true)
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
     * Length of the base name (result) string
     */
    private final int maxBaseNameLen;

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
    private final @Nullable String invalidCharRegex;

    public NameShortener(
        String propNamespaceRef,
        String propKeyRef,
        int maxLenRef,
        AccessContext accCtxRef,
        String delimiterRef,
        @Nullable String validCharacters
    )
    {
        propNamespace = propNamespaceRef;
        propKey = propKeyRef;
        maxLen = maxLenRef;
        accCtx = accCtxRef;
        delimiter = delimiterRef;
        delimiterLen = delimiterRef.length();

        maxBaseNameLen = maxLen - delimiterLen - 6;

        existingNames = new TreeSet<>();
        namePools = new TreeMap<>();
        numberPattern = Pattern.compile(delimiterRef + "([1-9]\\d{0,5})$");

        invalidCharRegex = validCharacters == null ? null : "[^" + validCharacters + "]";
    }

    public String shorten(ResourceDefinition rscDfn, String rscSuffix)
        throws AccessDeniedException, DatabaseException, LinStorException
    {
        return shorten(rscDfn.getProps(accCtx), "", rscDfn.getName().displayValue + rscSuffix, false);
    }

    public String shorten(VolumeDefinition vlmDfn, String keyPrefix, String rscSuffix, boolean appendVlmNr)
        throws AccessDeniedException, DatabaseException, LinStorException
    {
        String overrideVlmId = vlmDfn.getProps(accCtx).getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);
        String fullName;
        if (overrideVlmId != null)
        {
            fullName = overrideVlmId;
        }
        else
        {
            fullName = vlmDfn.getResourceDefinition().getName().displayValue + rscSuffix +
                (appendVlmNr ? String.format("_%05d", vlmDfn.getVolumeNumber().value) : "");
        }
        String shortName = shorten(
            vlmDfn.getProps(accCtx),
            keyPrefix,
            fullName,
            overrideVlmId != null
        );

        return shortName;
    }

    public String shorten(Props props, String propKeyPrefix, String fullName, boolean forceVolumeName)
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
                    final String baseName = getBaseName(ret);

                    // Name is too long or conflicts with an existing name
                    final NumberPool nrPool = getNumberPool(baseName, true);
                    int resourceNr = nrPool.autoAllocate(1, 999_999);
                    shortName = baseName + delimiter + resourceNr;

                    // Register the shortened name
                    existingNames.add(shortName);
                    // Set shortName property
                    props.setProp(localPropKey, shortName);
                }
                else
                {
                    // Full resource name is short enough and does not conflict with existing names
                    shortName = ret;

                    // Register the name as it is
                    existingNames.add(ret);
                    // Set shortName property
                    props.setProp(localPropKey, shortName);

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

            if (forceVolumeName && !shortName.equals(fullName))
            {
                throw new LinStorException(
                    "Cannot use forced volum id!",
                    "The given volume id " + fullName + " cannot be used",
                    "The given volume id " + fullName + " (possibly set via " +
                        ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID + ") is already used by a different volume-definition.",
                    "Ensure that \n" +
                        " * '" + fullName + "' is not already used by a different volume definition\n" +
                        " * '" + fullName + "' does not contain invalid characters for the used storage provider",
                    null
                );
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
            final String nrSuffix = nrMatcher.group(1);
            final String baseName = name.substring(0, name.length() - nrSuffix.length() - delimiterLen);

            // If there is a number pattern at the end of a name that is longer than the base name
            // for auto-numbered names, the numbers do not need to be tracked, because then it's
            // obviously not an auto-generated name/number combination
            if (baseName.length() <= maxBaseNameLen)
            {
                final int resourceNr = parseNumber(nrSuffix);

                final NumberPool nrPool = getNumberPool(baseName, true);
                nrPool.allocate(resourceNr);
            }
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
        final int fullNameLen = fullName.length();
        String baseName;
        if (fullNameLen <= maxLen)
        {
            // Find any existing number suffix
            Matcher nrMatcher = numberPattern.matcher(fullName);
            if (nrMatcher.find())
            {
                // Remove the number suffix
                final int splitIdx = fullName.lastIndexOf(delimiter);
                // If the name is still too long, shorten it to make it a suitable base name
                baseName = splitIdx <= maxBaseNameLen ?
                    fullName.substring(0, splitIdx) : fullName.substring(0, maxBaseNameLen);
            }
            else
            {
                baseName = fullNameLen <= maxBaseNameLen ? fullName : fullName.substring(0, maxBaseNameLen);
            }
        }
        else
        {
            // If the name, with or without a number suffix, is longer than the permitted maximum length,
            // then ignore what looks like a number suffix and shorten to base name length
            baseName = fullName.substring(0, maxBaseNameLen);
        }
        return baseName;
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
    private @Nullable NumberPool getNumberPool(String baseName, boolean createFlag)
    {
        NumberPool numberPool = namePools.get(baseName);
        if (numberPool == null && createFlag)
        {
            numberPool = new BitmapPool(1_000_000);
            namePools.put(baseName, numberPool);
        }
        return numberPool;
    }

    private void removeNumberPool(String baseName)
    {
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
        ReadOnlyProps rscDfnProps = rscDfn.getProps(accCtx);
        String shortName = rscDfnProps.getProp(propKey);
        if (shortName != null)
        {
            final String baseName = getBaseName(shortName);
            Matcher nrMatcher = numberPattern.matcher(shortName);
            if (nrMatcher.find())
            {
                final int resourceNr = parseNumber(nrMatcher.group(1));
                final NumberPool nrPool = getNumberPool(baseName, false);
                if (nrPool != null)
                {
                    nrPool.deallocate(resourceNr);
                    if (nrPool.isEmpty())
                    {
                        removeNumberPool(baseName);
                    }
                }
            }
            existingNames.remove(shortName);
        }
    }
}
