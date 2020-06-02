package com.linbit.utils;

import com.google.inject.Module;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.logging.ErrorReporter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

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
        final ErrorReporter errorLog
    )
    {
        boolean loaded = false;
        errorLog.logInfo("Attempting dynamic load of extension module \"%s\"", className);
        try
        {
            Class<?> injClass = Class.forName(className);
            Constructor<?> injModuleConstr = injClass.getDeclaredConstructor();
            Module injModule = (Module) injModuleConstr.newInstance();
            errorLog.logInfo("Dynamic load of extension module \"%s\" was successful", className);
            injModList.add(injModule);
            loaded = true;
        }
        catch (ClassNotFoundException exc)
        {
            errorLog.logInfo("Extension module \"%s\" is not installed", className);
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException |
               NoSuchMethodException | ClassCastException | LinkageError loadErr)
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
