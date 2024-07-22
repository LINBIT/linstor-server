package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;

import java.util.regex.Matcher;

public class DynamicNumberPoolImpl implements DynamicNumberPool
{
    private static final String IN_USE_EXC_FORMAT =
        " %d is already in use";

    private final ErrorReporter errorReporter;
    private final ReadOnlyProps ctrlConf;
    private final String ctrlConfKeyRange;
    private final String elementName;
    private final NumberRangeChecker rangeLimitChecker;
    private final int defaultMin;
    private final int defaultMax;

    private final NumberPool numberPool;

    private int rangeMin;
    private int rangeMax;

    public DynamicNumberPoolImpl(
        ErrorReporter errorReporterRef,
        ReadOnlyProps ctrlConfRef,
        String ctrlConfKeyRangeRef,
        String elementNameRef,
        NumberRangeChecker rangeLimitCheckerRef,
        int hardMax,
        int defaultMinRef,
        int defaultMaxRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlConf = ctrlConfRef;
        ctrlConfKeyRange = ctrlConfKeyRangeRef;
        elementName = elementNameRef;
        rangeLimitChecker = rangeLimitCheckerRef;
        defaultMin = defaultMinRef;
        defaultMax = defaultMaxRef;

        rangeMin = defaultMinRef;
        rangeMax = defaultMaxRef;

        numberPool = new BitmapPool(hardMax + 1);
    }

    @Override
    public void reloadRange()
    {
        String strRange;
        try
        {
            // it's possible that there are only the default values, so check for that
            if (ctrlConfKeyRange != null)
            {
                strRange = ctrlConf.getProp(ctrlConfKeyRange);
            }
            else
            {
                strRange = null;
            }
            Matcher matcher;
            boolean useDefaults = true;

            if (strRange != null)
            {
                matcher = NumberPoolModule.RANGE_PATTERN.matcher(strRange);
                if (matcher.find())
                {
                    try
                    {
                        rangeMin = Integer.parseInt(matcher.group("min"));
                        rangeMax = Integer.parseInt(matcher.group("max"));

                        rangeLimitChecker.check(rangeMin);
                        rangeLimitChecker.check(rangeMax);
                        useDefaults = false;
                    }
                    catch (ValueOutOfRangeException | NumberFormatException exc)
                    {
                        errorReporter.reportError(
                            exc,
                            null,
                            null,
                            "Unable to parse range from '" + strRange + "'"
                        );
                    }
                }
                else
                {
                    errorReporter.logError("Unable to extract range from '" + strRange + "'");
                }
            }
            if (useDefaults)
            {
                rangeMin = defaultMin;
                rangeMax = defaultMax;
            }
        }
        catch (InvalidKeyException invldKeyExc)
        {
            throw new ImplementationError(
                "Controller configuration key was invalid: " + invldKeyExc.invalidKey,
                invldKeyExc
            );
        }
    }

    @Override
    public void allocate(int nr)
        throws ValueInUseException
    {
        if (!numberPool.allocate(nr))
        {
            throw new ValueInUseException(String.format(elementName + IN_USE_EXC_FORMAT, nr));
        }
    }

    @Override
    public int autoAllocate()
        throws ExhaustedPoolException
    {
        return numberPool.autoAllocate(
            rangeMin,
            rangeMax
        );
    }

    @Override
    public void deallocate(int nr)
    {
        numberPool.deallocate(nr);
    }

    public interface NumberRangeChecker
    {
        void check(Integer integer)
            throws ValueOutOfRangeException;
    }
}
