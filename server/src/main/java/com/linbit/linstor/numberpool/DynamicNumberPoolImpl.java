package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.range.Range;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicNumberPoolImpl implements DynamicNumberPool
{
    private static final String IN_USE_EXC_FORMAT =
        " %d is already in use";

    private final ErrorReporter errorReporter;
    private final @Nullable PriorityProps prioProps;
    private final @Nullable String propKeyRange;
    private final @Nullable String elementName;
    private final @Nullable NumberRangeChecker rangeLimitChecker;
    private final int defaultMin;
    private final int defaultMax;

    private final NumberPool numberPool;

    private List<Range> ranges;
    private List<Range> blockedRanges;
    private List<Range> effectiveRanges;

    public DynamicNumberPoolImpl(
        ErrorReporter errorReporterRef,
        @Nullable PriorityProps prioPropsRef,
        @Nullable String propKeyRangeRef,
        @Nullable String elementNameRef,
        @Nullable NumberRangeChecker rangeLimitCheckerRef,
        int hardMax,
        int defaultMinRef,
        int defaultMaxRef
    )
    {
        errorReporter = errorReporterRef;
        prioProps = prioPropsRef;
        propKeyRange = propKeyRangeRef;
        elementName = elementNameRef;
        rangeLimitChecker = rangeLimitCheckerRef;
        defaultMin = defaultMinRef;
        defaultMax = defaultMaxRef;

        ranges = new ArrayList<>();
        ranges.add(new Range(defaultMinRef, defaultMaxRef));

        numberPool = new BitmapPool(hardMax + 1);
        blockedRanges = new ArrayList<>();
        effectiveRanges = new ArrayList<>();
    }

    @Override
    public void reloadRange()
    {
        @Nullable String strRange;
        try
        {
            // it's possible that there are only the default values, so check for that
            if (propKeyRange != null)
            {
                strRange = prioProps.getProp(propKeyRange);
            }
            else
            {
                strRange = null;
            }

            List<Range> newRanges;
            if (strRange == null)
            {
                newRanges = new ArrayList<>();
            }
            else
            {
                newRanges = Range.parseList(strRange);
                if (newRanges.isEmpty())
                {
                    errorReporter.logError("Unable to extract range from '" + strRange + "'");
                }
                else
                {
                    Iterator<Range> rangesIt = newRanges.iterator();
                    while (rangesIt.hasNext())
                    {
                        Range range = rangesIt.next();
                        try
                        {
                            rangeLimitChecker.check(range.from());
                            rangeLimitChecker.check(range.to());
                        }
                        catch (ValueOutOfRangeException exc)
                        {
                            errorReporter.reportError(
                                exc,
                                null,
                                null,
                                range + " outside of allowed range"
                            );
                            rangesIt.remove();
                        }
                    }
                }
            }
            if (newRanges.isEmpty())
            {
                newRanges.add(new Range(defaultMin, defaultMax));
            }
            ranges = newRanges;
            recaculateEffecitveRanges();
        }
        catch (InvalidKeyException invldKeyExc)
        {
            throw new ImplementationError(
                "Controller configuration key was invalid: " + invldKeyExc.invalidKey,
                invldKeyExc
            );
        }
    }

    public void setBlockedRanges(List<Range> blockedRangesRef)
    {
        blockedRanges = blockedRangesRef;
        recaculateEffecitveRanges();
    }

    private void recaculateEffecitveRanges()
    {
        effectiveRanges = RangeUtils.subtract(ranges, blockedRanges);
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
    public boolean tryAllocate(int nr)
    {
        return numberPool.allocate(nr);
    }

    @Override
    public int autoAllocate()
        throws ExhaustedPoolException
    {
        return numberPool.autoAllocate(effectiveRanges);
    }

    @Override
    public void deallocate(int nr)
    {
        numberPool.deallocate(nr);
    }

    public interface NumberRangeChecker
    {
        void check(int integer)
            throws ValueOutOfRangeException;
    }
}
