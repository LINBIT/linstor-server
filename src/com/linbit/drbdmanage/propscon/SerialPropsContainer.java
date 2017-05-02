package com.linbit.drbdmanage.propscon;

import com.linbit.ImplementationError;
import java.util.Map;
import java.util.Set;

/**
 * Serial number-tracking properties container
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SerialPropsContainer extends PropsContainer
{
    private SerialGenerator serialGen;

    public static SerialPropsContainer createRootContainer()
    {
        SerialPropsContainer con = null;
        try
        {
            con = new SerialPropsContainer();
        }
        catch (InvalidKeyException keyExc)
        {
            // If root container creation generates an InvalidKeyException,
            // that is always a bug in the implementation
            throw new ImplementationError(
                "Root container creation generated an exception",
                keyExc
            );
        }
        return con;
    }

    public static SerialPropsContainer createRootContainer(SerialGenerator sGen)
    {
        SerialPropsContainer con = null;
        try
        {
            con = new SerialPropsContainer(sGen);
        }
        catch (InvalidKeyException keyExc)
        {
            // If root container creation generates an InvalidKeyException,
            // that is always a bug in the implementation
            throw new ImplementationError(
                "Root container creation generated an exception",
                keyExc
            );
        }
        return con;
    }

    SerialPropsContainer()
        throws InvalidKeyException
    {
        this(null, null, null);
    }

    SerialPropsContainer(SerialGenerator sGen)
        throws InvalidKeyException
    {
        this(null, null, sGen);
    }

    SerialPropsContainer(String key, PropsContainer parent, SerialGenerator sGen)
        throws InvalidKeyException
    {
        super(key, parent);
        if (sGen == null)
        {
            serialGen = new SeqSerialGenerator(new SerialAccessorImpl(this));
        }
        else
        {
            serialGen = sGen;
        }
        serialGen.peekSerial();
    }

    public SerialGenerator getSerialGenerator()
    {
        return serialGen;
    }

    @Override
    public String setProp(String key, String value, String namespace)
        throws InvalidKeyException, InvalidValueException
    {
        String oldValue = super.setProp(key, value, namespace);
        setSerial(serialGen.newSerial());
        return oldValue;
    }

    @Override
    public boolean setAllProps(Map<? extends String, ? extends String> entryMap, String namespace)
        throws InvalidKeyException, InvalidValueException
    {
        boolean changed = false;
        if (!entryMap.isEmpty())
        {
            changed = super.setAllProps(entryMap, namespace);
            setSerial(serialGen.newSerial());
        }
        return changed;
    }

    @Override
    public String removeProp(String key, String namespace)
        throws InvalidKeyException
    {
        String value = super.removeProp(key, namespace);
        if (value != null)
        {
            setSerial(serialGen.newSerial());
        }
        return value;
    }

    @Override
    boolean removeAllProps(Set<String> selection, String namespace)
    {
        boolean changed = false;
        changed = super.removeAllProps(selection, namespace);
        if (changed)
        {
            setSerial(serialGen.newSerial());
        }
        return changed;
    }

    @Override
    boolean retainAllProps(Set<String> selection, String namespace)
    {
        boolean changed = false;
        changed = super.retainAllProps(selection, namespace);
        if (changed)
        {
            setSerial(serialGen.newSerial());
        }
        return changed;
    }

    @Override
    public void clear()
    {
        // Cache the serial number, because the superclass' clear() method
        // empties the container completely
        long serial = getSerial();
        super.clear();
        // Restore the cached serial number
        setSerial(serial);
        // Update the serial number
        setSerial(serialGen.newSerial());
    }

    @Override
    SerialPropsContainer createSubContainer(String key, PropsContainer con) throws InvalidKeyException
    {
        return new SerialPropsContainer(key, con, serialGen);
    }

    public void closeGeneration()
    {
        serialGen.closeGeneration();
    }

    private long getSerial()
    {
        long serial = 0;
        try
        {
            String serialStr = super.getProp(SerialGenerator.KEY_SERIAL, "/");
            if (serialStr != null)
            {
                try
                {
                    serial = Long.parseLong(serialStr);
                }
                catch (NumberFormatException ignored)
                {
                }
            }
            else
            {
                super.setProp(SerialGenerator.KEY_SERIAL, "0", "/");
            }
        }
        catch (InvalidKeyException keyExc)
        {
            throw new ImplementationError(
                String.format("KEY_SERIAL constant value of '%s' is an invalid key"),
                keyExc
            );
        }
        catch (InvalidValueException valueExc)
        {
            throw new ImplementationError(
                "KEY_SERIAL assigned value of '0' is an invalid value",
                valueExc
            );
        }
        return serial;
    }

    private void setSerial(long serial)
    {
        String serialStr = null;
        try
        {
            serialStr = Long.toString(serial);
            super.setProp(SerialGenerator.KEY_SERIAL, serialStr, "/");
        }
        catch (InvalidKeyException keyExc)
        {
            throw new ImplementationError(
                String.format("KEY_SERIAL constant value of '%s' is an invalid key"),
                keyExc
            );
        }
        catch (InvalidValueException valueExc)
        {
            throw new ImplementationError(
                String.format(
                    "Serial number value %d, serialized as \"%s\", is an invalid value"
                ),
                valueExc
            );
        }
    }

    private static class SerialAccessorImpl implements SerialAccessor
    {
        private final SerialPropsContainer propsCon;

        private SerialAccessorImpl(SerialPropsContainer propsCon)
        {
            this.propsCon = propsCon;
        }

        @Override
        public final long getSerial()
        {
            return propsCon.getSerial();
        }

        @Override
        public final void setSerial(long serial)
        {
            propsCon.setSerial(serial);
        }
    }
}
