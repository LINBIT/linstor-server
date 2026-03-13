package com.linbit;

import com.linbit.SizeConv.SizeUnit;


public sealed interface SizeSpec
{
    record Abs(long num, SizeUnit unit) implements SizeSpec
    {
    }

    record Percent(float num) implements SizeSpec
    {
    }
}
