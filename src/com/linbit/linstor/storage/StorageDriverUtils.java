package com.linbit.linstor.storage;

import com.linbit.linstor.SatelliteCoreServices;

public class StorageDriverUtils
{
    public static StorageDriver createInstance(String simpleName, SatelliteCoreServices coreSvc)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, StorageException
    {
        StorageDriver driver = null;

        if (simpleName != null)
        {
            String fullQualifiedName = StorageDriverUtils.class.getPackage().getName() + "." + simpleName;
            Class<?> driverClass = Class.forName(fullQualifiedName);
            if (!StorageDriver.class.isAssignableFrom(driverClass))
            {
                throw new ClassCastException("Class does not implement the StorageDriver interface: " + simpleName);
            }
            driver = (StorageDriver) driverClass.newInstance();
            driver.initialize(coreSvc);
        }

        return driver;
    }

    private StorageDriverUtils()
    {
    }
}
