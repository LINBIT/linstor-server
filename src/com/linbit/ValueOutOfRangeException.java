package com.linbit;

public class ValueOutOfRangeException extends Exception
{
    public enum ViolationType
    {
        GENERIC,
        TOO_LOW,
        TOO_HIGH
    }

    private final ViolationType violation;

    public ValueOutOfRangeException(ViolationType violationSpec)
    {
        violation = violationSpec;
    }

    public ValueOutOfRangeException(String message, ViolationType violationSpec)
    {
        super(message);
        violation = violationSpec;
    }

    public ViolationType getViolationType()
    {
        return violation;
    }
}
