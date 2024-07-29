package com.linbit.utils;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.logging.ErrorReporter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import com.google.inject.Module;

public class InjectorLoader
{

    /**
     * Dynamically loads an injector module class and instantiates it
     *
     * The injector module class is expected to have a default constructor, which will be called to
     * create an instance of the class.
     *
     * @param className Class name of the module to load
     * @param errorLog Reference to the error reporter instance
     * @return Object instance of the specified class, or null if loading failed
     */
    public static boolean dynLoadInjModule(
        final String className,
        final List<Module> injModList,
        final ErrorReporter errorLog,
        @Nullable final DatabaseDriverInfo.DatabaseType dbType
    )
    {
        boolean loaded = false;
        errorLog.logInfo("Attempting dynamic load of extension module \"%s\"", className);
        try
        {
            Class<?> injClass = Class.forName(className);
            Constructor<?> injModuleConstr;
            try
            {
                injModuleConstr = injClass.getDeclaredConstructor(DatabaseDriverInfo.DatabaseType.class);
            }
            catch (NoSuchMethodException constrExc)
            {
                injModuleConstr = injClass.getDeclaredConstructor();
            }
            Class<?>[] constrParams = injModuleConstr.getParameterTypes();
            Module injModule;
            if (constrParams.length >= 1 && constrParams[0] == DatabaseDriverInfo.DatabaseType.class)
            {
                errorLog.logDebug("Constructing instance of module \"%s\" with database type parameter", className);
                injModule  = (Module) injModuleConstr.newInstance(dbType);
            }
            else
            {
                errorLog.logDebug("Constructing instance of module \"%s\" with default constructor", className);
                injModule  = (Module) injModuleConstr.newInstance();
            }
            errorLog.logInfo("Dynamic load of extension module \"%s\" was successful", className);
            injModList.add(injModule);
            loaded = true;
        }
        catch (ClassNotFoundException exc)
        {
            errorLog.logInfo("Extension module \"%s\" is not installed", className);
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException |
               NoSuchMethodException | ClassCastException  loadErr)
        {
            errorLog.reportError(
                new LinStorException(
                    "Module \"" + className + "\" failed to load due to an initialization error",
                    "Module \"" + className + "\" could not be loaded",
                    "The class loader could not initialize the injector module",
                    "Please provide this error report to LINBIT support",
                    null,
                    loadErr
                )
            );
        }
        return loaded;
    }
}
