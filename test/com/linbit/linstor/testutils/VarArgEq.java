package com.linbit.linstor.testutils;

import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.Equality;
import org.mockito.internal.matchers.VarargMatcher;

import java.util.Arrays;

import static org.mockito.Matchers.argThat;

public class VarArgEq<T> extends ArgumentMatcher<T[]> implements VarargMatcher
{
    public static <T> T[] varArgEq(T[] expected)
    {
        argThat(new VarArgEq<>(expected));
        return null;
    }

    private final T[] wanted;

    private VarArgEq(T[] wanted)
    {
        this.wanted = wanted;
    }

    @Override
    public boolean matches(Object object)
    {
        return Equality.areEqual(wanted, object);
    }

    @Override
    public void describeTo(Description description)
    {
        description.appendText(Arrays.toString(wanted));
    }
}
