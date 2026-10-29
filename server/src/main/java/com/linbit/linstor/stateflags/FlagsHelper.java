package com.linbit.linstor.stateflags;

import com.linbit.utils.PairNonNull;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
            value |= valueOf(enumClass, sFlag).getFlagValue();
        }

        return value;
    }

    /**
     * The flag named as {@link #toStringList} names it, i.e. by its {@link Object#toString()}, which differs from the
     * name of the enum constant for flags that are renamed only within the server (see
     * {@code ResourceDefinition.Flags}).
     *
     * @throws IllegalArgumentException if there is no such flag
     */
    private static <E extends Enum<E> & Flags> E valueOf(Class<E> enumClass, String flagName)
    {
        return Arrays.stream(enumClass.getEnumConstants())
            .filter(flag -> flag.toString().equals(flagName))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("No flag " + flagName + " in " + enumClass.getCanonicalName())
            );
    }

    @SuppressWarnings("unchecked")
    public static <E extends Enum<E> & Flags> E[] toFlagsArray(
        Class<E> enumClass,
        StateFlags<E> flags
    )
    {
        EnumSet<E> values = EnumSet.allOf(enumClass);
        List<E> setFlags = new ArrayList<>();
        for (E val : values)
        {
            if (flags.isSet(val))
            {
                setFlags.add(val);
            }
        }
        return setFlags.toArray((E[]) Array.newInstance(enumClass, setFlags.size()));
    }

    public static <E extends Enum<E> & Flags> boolean isFlagEnabled(
        long flagBits,
        E... flags
    )
    {
        long tmp = 0;
        for (E flag : flags)
        {
            tmp |= flag.getFlagValue();
        }
        return (flagBits & tmp) == tmp;
    }

    /**
     * Returns a pair of sets of positive and negative flags.
     * pair.a is the set of positive flags, pair.b is the set of negative flags.
     */
    public static <E extends Enum<E> & Flags> PairNonNull<Set<E>, Set<E>> extractFlagsToEnableOrDisable(
        Class<E> classRef,
        List<String> flagsListRef
    )
    {
        Set<E> flagsToEnable = new TreeSet<>();
        Set<E> flagsToDisable = new TreeSet<>();
        PairNonNull<Set<E>, Set<E>> ret = new PairNonNull<>(flagsToEnable, flagsToDisable);

        for (String flag : flagsListRef)
        {
            if (flag.startsWith("-"))
            {
                flagsToDisable.add(valueOf(classRef, flag.substring(1)));
            }
            else
            {
                flagsToEnable.add(valueOf(classRef, flag));
            }
        }
        return ret;
    }

    @SafeVarargs
    public static <E extends Enum<E> & Flags> long getBits(E... flags)
    {
        long bits = 0;
        for (E flag : flags)
        {
            bits |= flag.getFlagValue();
        }
        return bits;
    }

    private FlagsHelper()
    {
    }

}
