package com.linbit.linstor.storage;

public class StorageDriverUtils
{
    public static StorageDriver createInstance(String simpleName)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException
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
        }

        return driver;
    }
}
