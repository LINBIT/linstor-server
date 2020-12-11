package com.linbit.linstor.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
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
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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

    /**
     * The property key used for lookup of already shortened name
     */
    private final String key;
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
    private final Map<Pair<String, Integer>, NumberPool> namePools;
    /**
     * Set will contain shortened as well as rscNames that did not have to
     * be shortened. Used to detect any conflicting resource names
     */
    private final Set<String> existingNames;
    /**
     * delimiter + "(\\d+)$"
     */
    private final Pattern numberPattern;

    /**
     * "[^" + validCharacters (from constructor parameter) + "]"
     * Used to replace everything that matches this regex with empty string.
     * If null, the replacement is skipped
     */
    private final String invalidCharRegex;

    public NameShortener(
        String keyRef,
        int maxLenRef,
        AccessContext accCtxRef,
        String delimiterRef,
        String validCharacters
    )
    {
        key = keyRef;
        maxLen = maxLenRef;
        accCtx = accCtxRef;
        delimiter = delimiterRef;
        delimiterLen = delimiterRef.length();

        existingNames = new TreeSet<>();
        namePools = new TreeMap<>();
        numberPattern = Pattern.compile(delimiterRef + "(\\d+)$");

        invalidCharRegex = validCharacters == null ? null : "[^" + validCharacters + "]";
    }

    public String shorten(ResourceDefinition rscDfn, String rscSuffix)
        throws AccessDeniedException, DatabaseException
    {
        return shorten(rscDfn.getProps(accCtx), "", rscDfn.getName().displayValue + rscSuffix);
    }

    public String shorten(VolumeDefinition vlmDfn, String keyPrefix, String rscSuffix)
        throws AccessDeniedException, DatabaseException
    {
        return shorten(vlmDfn.getProps(accCtx), key, vlmDfn.getResourceDefinition().getName().displayValue + rscSuffix);
    }

    private String shorten(Props props, String propKeyPrefix, String fullName)
        throws AccessDeniedException, DatabaseException
    {
        String shortName = null;
        try
        {
            String ret = props.getProp(propKeyPrefix + key);
            if (ret == null)
            {
                // rscDfn does not have a property with the shortendName yet

                ret = fullName;
                if (invalidCharRegex != null)
                {
                    ret = ret.replaceAll(invalidCharRegex, "");
                }

                if (ret.length() > maxLen || existingNames.contains(ret))
                {
                    // either too long or conflicting with existing / known names

                    int digitCount = 1;
                    if (existingNames.contains(ret))
                    {
                        /*
                         * if the name is already reserved, we need to check if that name
                         * ends with our number pattern. This only helps to calculate a better
                         * starting digitCount, rather than starting with 1
                         */
                        Matcher m = numberPattern.matcher(ret);
                        if (m.find())
                        {
                            digitCount = m.group(1).length();
                        }
                    }
                    boolean retry = true;
                    while (retry)
                    {
                        NumberPool numberPool = getNumberPool(ret, digitCount);

                        int poolMin = base10pow(digitCount);
                        int poolMax = poolMin * 10;

                        try
                        {
                            // try to allocate new number or throw exception when no numbers are available
                            int num = numberPool.autoAllocate(poolMin, poolMax - 1);

                            // if succeeded set property and reserve the name
                            String baseName = getBaseName(ret, digitCount);
                            shortName = baseName + delimiter + num;
                            existingNames.add(shortName);
                            props.setProp(key, shortName);

                            // we are done, we could basically return here
                            retry = false;
                        }
                        catch (ExhaustedPoolException exc)
                        {
                            // retry with higher digitCount
                            digitCount++;
                        }
                    }
                }
                else
                {
                    // fullName is short enough and is not already reserved

                    // reserve name
                    existingNames.add(ret);
                    shortName = ret;
                    // make sure property is set
                    props.setProp(key, shortName);

                    // allocate number in case it might conflict our numberPattern
                    Matcher m = numberPattern.matcher(ret);
                    if (m.find())
                    {
                        int digitCount = m.group(1).length();
                        int num = Integer.parseInt(m.group(1));

                        allocate(ret, digitCount, num);
                    }
                }
            }
            else
            {
                // property is already set. mostly noop here
                shortName = ret;

                // in case the property was set in a previous controller-session
                existingNames.add(shortName);

                // make sure the number is allocated
                Matcher m = numberPattern.matcher(ret);
                if (m.find())
                {
                    int digitCount = m.group(1).length();
                    int num = Integer.parseInt(m.group(1));

                    allocate(ret, digitCount, num);
                }
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return shortName;
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
    private String getBaseName(String fullName, int digitCount)
    {
        return fullName.substring(
            0,
            Math.min(maxLen - digitCount - delimiterLen, fullName.length())
        );
    }

    /**
     * Allocates the given number in the numberPool returned by {@link #getNumberPool(String, int)}.
     * This method silently ignores the case if the number was already allocated.
     *
     * @param fullName
     * @param digitCount
     * @param num
     */
    private void allocate(String fullName, int digitCount, int num)
    {
        NumberPool numberPool = getNumberPool(fullName, digitCount);
        numberPool.allocate(num);
    }

    private void deallocate(String fullName, int digitCount, int num)
    {
        NumberPool numberPool = getNumberPool(fullName, digitCount);
        numberPool.deallocate(num);
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
    private NumberPool getNumberPool(String fullName, int digitCount)
    {
        String baseName = getBaseName(fullName, digitCount);
        Pair<String, Integer> namePoolKey = new Pair<>(baseName, digitCount);
        NumberPool numberPool = namePools.get(namePoolKey);
        if (numberPool == null)
        {
            numberPool = new BitmapPool(base10pow(digitCount + 1));
            namePools.put(namePoolKey, numberPool);
        }
        return numberPool;
    }

    private int base10pow(int exp)
    {
        int tmp = 1;
        for (int idx = 1; idx < exp; idx++)
        {
            tmp *= 10;
        }
        return tmp;
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
        String shortName = rscDfnProps.getProp(key);
        if (shortName != null)
        {
            Matcher m = numberPattern.matcher(shortName);
            if (m.find())
            {
                int digitCount = m.group(1).length();
                int num = Integer.parseInt(m.group(1));

                deallocate(shortName, digitCount, num);
            }
            existingNames.remove(shortName);
        }
    }
}
