package com.linbit.linstor.api.prop;

public class StringProperty extends GenericProperty implements Property
{

    public StringProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        String infoRef,
        String unitRef,
        String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
    }

    @Override
    public boolean isValid(String value)
    {
        return true;
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.STRING;
    }

    @Override
    public String getErrorMsg()
    {
        return "Congratulations, you found an easter-egg. " +
            "Now please report to the deveolpers how you actually got here, " +
            "or did you cheat and just read the source-code?";
    }

}
