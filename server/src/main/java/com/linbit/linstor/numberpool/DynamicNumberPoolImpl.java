package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.range.Range;
import com.linbit.linstor.range.RangeUtils;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicNumberPoolImpl implements DynamicNumberPool
{
    private final ErrorReporter errorReporter;
    private final @Nullable PriorityProps prioProps;
    private final @Nullable String propKeyRange;
    private final @Nullable String propKeyBlockedRange;
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
        @Nullable String propKeyBlockedRangeRef,
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
        propKeyBlockedRange = propKeyBlockedRangeRef;
        elementName = elementNameRef;
        rangeLimitChecker = rangeLimitCheckerRef;
        defaultMin = defaultMinRef;
        defaultMax = defaultMaxRef;

        ranges = new ArrayList<>();

        numberPool = new BitmapPool(hardMax + 1);
        blockedRanges = new ArrayList<>();

        try
        {
            loadRanges(null);
            loadBlockedRanges(null);
        }
        catch (DatabaseException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        recaculateEffecitveRanges();
    }

    private void loadRanges(@Nullable Props propsToUpdateRef) throws DatabaseException, AccessDeniedException
    {
        List<Range> newRanges = parseRange(propKeyRange, true, propsToUpdateRef);
        if (newRanges.isEmpty())
        {
            newRanges.add(new Range(defaultMin, defaultMax));
        }
        ranges = newRanges;
    }

    private void loadBlockedRanges(@Nullable Props propsToUpdateRef) throws DatabaseException, AccessDeniedException
    {
        blockedRanges = parseRange(propKeyBlockedRange, false, propsToUpdateRef);
    }

    @Override
    public void reloadRange(@Nullable Props propsToUpdateRef) throws DatabaseException, AccessDeniedException
    {
        loadRanges(propsToUpdateRef);
        recaculateEffecitveRanges();
    }

    @Override
    public void reloadBlockedRange(@Nullable Props propsToUpdateRef) throws DatabaseException, AccessDeniedException
    {
        loadBlockedRanges(propsToUpdateRef);
        recaculateEffecitveRanges();
    }

    private List<Range> parseRange(
        final String propKeyRef,
        final boolean runRangeChecksRef,
        final @Nullable Props propsToUpdateRef
    )
        throws DatabaseException, AccessDeniedException
    {
        @Nullable String strRange;
        try
        {
            // it's possible that there are only the default values, so check for that
            if (propKeyRef != null)
            {
                strRange = prioProps.getProp(propKeyRef);
            }
            else
            {
                strRange = null;
            }
        }
        catch (InvalidKeyException invldKeyExc)
        {
            throw new ImplementationError(
                "Controller configuration key was invalid: " + invldKeyExc.invalidKey,
                invldKeyExc
            );
        }

        List<Range> parsedRanges;
        if (strRange == null)
        {
            parsedRanges = new ArrayList<>();
        }
        else
        {
            parsedRanges = Range.parseList(strRange);
            if (parsedRanges.isEmpty())
            {
                errorReporter.logError("Unable to extract range from '%s' (Prop: '%s')", strRange, propKeyRef);
            }
            else
            {
                if (runRangeChecksRef)
                {
                    checkRange(parsedRanges);
                }
            }

            if (propsToUpdateRef != null)
            {
                String simplifiedReRender = RangeUtils.render(parsedRanges);
                if (!simplifiedReRender.equals(strRange))
                {
                    try
                    {
                        propsToUpdateRef.setProp(propKeyRef, simplifiedReRender);
                    }
                    catch (InvalidKeyException | InvalidValueException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
            }
        }
        return parsedRanges;
    }

    private void checkRange(List<Range> parsedRanges)
    {
        Iterator<Range> rangesIt = parsedRanges.iterator();
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

    public void setBlockedRanges(List<Range> blockedRangesRef)
    {
        blockedRanges = blockedRangesRef;
        recaculateEffecitveRanges();
    }

    private void recaculateEffecitveRanges()
    {
        effectiveRanges = RangeUtils.subtract(ranges, blockedRanges);
    }

    public boolean isBlocked(int nrRef)
    {
        boolean ret = false;
        for (Range blockedRange : blockedRanges)
        {
            if (blockedRange.contains(nrRef))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    @Override
    public void allocate(int nr)
        throws ValueInUseException
    {
        if (isBlocked(nr))
        {
            throw new ValueInUseException(String.format("%s %d is marked blocked", elementName, nr));
        }
        if (!numberPool.allocate(nr))
        {
            throw new ValueInUseException(String.format("%s %d is already in use", elementName, nr));
        }
    }

    @Override
    public boolean tryAllocate(int nr)
    {
        boolean ret;
        if (isBlocked(nr))
        {
            ret = false;
        }
        else
        {
            ret = numberPool.allocate(nr);
        }
        return ret;
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
        // we can / should allow to deallocate a port even if it is marked as being blocked
        numberPool.deallocate(nr);
    }

    public interface NumberRangeChecker
    {
        void check(int integer)
            throws ValueOutOfRangeException;
    }
}
