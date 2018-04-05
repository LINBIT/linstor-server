package com.linbit.linstor.core;


import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StderrErrorReporter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

/**
 *
 * @author rpeinthor
 */
public class ApiCallLoadingTest
{
    @Test
    public void testClassPathExpand() throws IOException
    {
        ErrorReporter errorReporter = new StderrErrorReporter("LINSTOR-UNITTESTS");

        {
            String cp1 = "." + File.pathSeparator + "bin";
            List<String> p1 = new ApiCallLoader(errorReporter).expandClassPath(cp1);
            ArrayList<String> a1 = new ArrayList<>();
            a1.add(Paths.get(".").toAbsolutePath().toString());
            a1.add(Paths.get("bin").toAbsolutePath().toString());
            assertArrayEquals(a1.toArray(), p1.toArray());
        }

        {
            String cp = "." + File.pathSeparator + "build" + File.separator + "libs" + File.separator + "*.jar";
            List<String> p = new ApiCallLoader(errorReporter).expandClassPath(cp);
            ArrayList<String> a = new ArrayList<>();
            a.add(Paths.get(".").toAbsolutePath().toString());
            assertArrayEquals("*.jar is not supported", a.toArray(), p.toArray());
        }

        {
            String cp = "." + File.pathSeparator + "build" + File.separator + "libs" + File.separator + "*";
            List<String> cpPaths = new ApiCallLoader(errorReporter).expandClassPath(cp);
            ArrayList<String> expArr = new ArrayList<>();
            expArr.add(Paths.get(".").toAbsolutePath().toString());
            expArr.addAll(Files.list(Paths.get("build/libs"))
                .filter(p -> p.toString().endsWith(".jar"))
                .map(p -> p.toAbsolutePath().toString())
                .collect(toList())
            );
            assertArrayEquals(expArr.toArray(), cpPaths.toArray());
        }

        {
            String cp = "." + File.pathSeparator + "*";
            List<String> p = new ApiCallLoader(errorReporter).expandClassPath(cp);
            ArrayList<String> a = new ArrayList<>();
            a.add(Paths.get(".").toAbsolutePath().toString());
            assertArrayEquals(a.toArray(), p.toArray());
        }
    }
}
