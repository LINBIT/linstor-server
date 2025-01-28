package com.linbit.linstor.core.utils;

import javax.annotation.Nullable;

import java.util.function.Function;
import java.util.function.Supplier;

public class NullableUtils
{
    private NullableUtils()
    {
        // utils class
    }

    public static <T, R> R mapNullable(
        @Nullable T elementRef,
        Supplier<R> supplierIfElementNullRef,
        Function</* @Nonnull */ T, R> mapperFunctionIfNonNullRef
    )
    {
        return elementRef == null ? supplierIfElementNullRef.get() : mapperFunctionIfNonNullRef.apply(elementRef);
    }

    public static <T> T orElse(@Nullable T elementRef, T defaultBoolRef)
    {
        return elementRef == null ? defaultBoolRef : elementRef;
    }

}
