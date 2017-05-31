package com.linbit.drbdmanage.propscon;

import com.linbit.ImplementationError;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Serial number-tracking properties container
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SerialPropsContainer extends PropsContainer
{
    private SerialGenerator serialGen;

    public static SerialPropsContainer createRootContainer() throws SQLException
    {
        return createRootContainer((SerialGenerator) null, null);
    }

    public static SerialPropsContainer createRootContainer(PropsConDatabaseDriver dbDriver) throws SQLException
    {
        return createRootContainer(null, dbDriver);
    }

    public static SerialPropsContainer createRootContainer(SerialGenerator sGen) throws SQLException
    {
        return createRootContainer(sGen, null);
    }

    public static SerialPropsContainer createRootContainer(SerialGenerator sGen, PropsConDatabaseDriver dbDriver)
        throws SQLException
    {
        SerialPropsContainer con = null;
        try
        {
            if (sGen == null)
            {
                con = new SerialPropsContainer();
            }
            else
            {
                con = new SerialPropsContainer(sGen);
            }
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
        con.dbDriver = dbDriver;
        return con;
    }

    public static SerialPropsContainer loadContainer(PropsConDatabaseDriver dbDriver) throws SQLException, InvalidKeyException, InvalidValueException
    {
        return loadContainer(dbDriver, null);
    }

    public static SerialPropsContainer loadContainer(PropsConDatabaseDriver dbDriver, SerialGenerator sGen) throws SQLException, InvalidKeyException, InvalidValueException
    {
        SerialPropsContainer container = createRootContainer(sGen);
        container.dbDriver = dbDriver;
        Map<String, String> loadedProps = dbDriver.load();

        // first, restore the properties

        // we should skip the .setAllProps method as that triggers a db re-persist
        for (Entry<String, String> entry : loadedProps.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            SerialPropsContainer targetContainer = container;
            int idx = key.lastIndexOf("/");
            if (idx != -1)
            {
                targetContainer = (SerialPropsContainer) container.ensureNamespaceExists(key.substring(0, idx));
            }
            String oldValue = targetContainer.getRawPropMap().put(key, value);
            if (oldValue == null)
            {
                targetContainer.modifySize(1);
            }
        }
        return container;
    }

    SerialPropsContainer()
        throws InvalidKeyException, SQLException
    {
        this(null, null, null);
    }

    SerialPropsContainer(SerialGenerator sGen)
        throws InvalidKeyException, SQLException
    {
        this(null, null, sGen);
    }

    SerialPropsContainer(String key, PropsContainer parent, SerialGenerator sGen)
        throws InvalidKeyException, SQLException
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
        throws InvalidKeyException, InvalidValueException, SQLException
    {
        String oldValue = super.setProp(key, value, namespace);
        setSerial(serialGen.newSerial());
        return oldValue;
    }

    @Override
    public boolean setAllProps(Map<? extends String, ? extends String> entryMap, String namespace)
        throws InvalidKeyException, InvalidValueException, SQLException
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
        throws InvalidKeyException, SQLException
    {
        String value = super.removeProp(key, namespace);
        if (value != null)
        {
            setSerial(serialGen.newSerial());
        }
        return value;
    }

    @Override
    boolean removeAllProps(Set<String> selection, String namespace) throws SQLException
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
    boolean retainAllProps(Set<String> selection, String namespace) throws SQLException
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
    public void clear() throws SQLException
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
    SerialPropsContainer createSubContainer(String key, PropsContainer con) throws InvalidKeyException, SQLException
    {
        return new SerialPropsContainer(key, con, serialGen);
    }

    public void closeGeneration()
    {
        serialGen.closeGeneration();
    }

    private long getSerial() throws SQLException
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

    private void setSerial(long serial) throws SQLException
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
        public final long getSerial() throws SQLException
        {
            return propsCon.getSerial();
        }

        @Override
        public final void setSerial(long serial) throws SQLException
        {
            propsCon.setSerial(serial);
        }
    }
}
