package com.linbit.linstor.stateflags;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * FlagsHelper contains static helper methods to avoid code duplication in flag enums.
 * @author rp
 */
public class FlagsHelper
{
    public static <E extends Enum<E> & Flags> List<String> toStringList(
        Class<E> enumClass,
        long rscFlags
    )
    {
        EnumSet<E> values = EnumSet.allOf(enumClass);
        List<String> strList = new ArrayList<>(values.size());
        for (E en : values)
        {
            if ((rscFlags & en.getFlagValue()) == en.getFlagValue())
            {
                strList.add(en.toString());
            }
        }
        return strList;
    }

    public static <E extends Enum<E> & Flags> long fromStringList(
        Class<E> enumClass,
        List<String> listFlags
    )
    {
        long value = 0;

        for (String sFlag : listFlags)
        {
            value |= E.valueOf(enumClass, sFlag).getFlagValue();
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    public static <E extends Enum<E> & Flags> E[] toFlagsArray(
        Class<E> enumClass,
        StateFlags<E> flags,
        AccessContext accCtx
    )
        throws AccessDeniedException
    {
        EnumSet<E> values = EnumSet.allOf(enumClass);
        List<E> setFlags = new ArrayList<>();
        for (E val : values)
        {
            if (flags.isSet(accCtx, val))
            {
                setFlags.add(val);
            }
        }
        return setFlags.toArray((E[]) Array.newInstance(enumClass, setFlags.size()));
    }
}
