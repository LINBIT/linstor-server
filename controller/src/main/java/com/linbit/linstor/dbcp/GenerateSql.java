package com.linbit.linstor.dbcp;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbcp.migration.LinstorMigration;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.GeneratedCrdJavaClass;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.GeneratedCrdResult;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.GeneratedResources;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.flywaydb.core.Flyway;

public class GenerateSql
{
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "linstor");
        props.setProperty("password", "linstor");
        ConnectionFactory connFactory = new DriverManagerConnectionFactory("jdbc:h2:mem:testDB", props);
        PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

        GenericObjectPoolConfig<PoolableConnection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMinIdle(10);
        poolConfig.setMaxIdle(100);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setFairness(true);
        GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

        poolConnFactory.setPool(connPool);
        poolConnFactory.setValidationQueryTimeout(60000);
        poolConnFactory.setMaxOpenPrepatedStatements(100);
        poolConnFactory.setMaxConnLifetimeMillis(3600000);
        poolConnFactory.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        try (PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connPool))
        {
            initJclCrypto();

            Flyway flyway = Flyway.configure()
                .schemas(DATABASE_SCHEMA_NAME)
                .dataSource(dataSource)
                .table("FLYWAY_SCHEMA_HISTORY")
                // When migrations are added in branches they can be applied in different orders
                .outOfOrder(true)
                // Pass the DB type to the migrations
                .placeholders(Map.of(LinstorMigration.PLACEHOLDER_KEY_DB_TYPE, "h2"))
                .locations(LinstorMigration.class.getPackage().getName().replace(".", "/"))
                .load();

            flyway.baseline();
            flyway.migrate();

            DatabaseConstantsGenerator gen = new DatabaseConstantsGenerator(dataSource.getConnection());
            String gitRoot = getGitRoot();
            String genDbTablesJavaCode = renderGeneratedDatabaseTables(gen, gitRoot);
            renderGeneratedKubernetesCustomResourceDefinitions(gen, gitRoot, genDbTablesJavaCode);
        }

        System.out.println("Classes generated successfully.");
    }

    private static void initJclCrypto()
        throws URISyntaxException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        Path jclPrjPath = Paths.get(GenerateSql.class.getProtectionDomain().getCodeSource().getLocation().toURI())
            .getParent() // undo "main"
            .getParent() // undo "bin"
            .getParent() // undo "controller"
            .resolve(Paths.get("jclcrypto", "bin", "main"));

        try (
            URLClassLoader cl = new URLClassLoader(
                new URL[]
                {
                    jclPrjPath.toUri().toURL()
                }
            );
        )
        {
            Class<?> jclCryptoProviderCls = cl.loadClass("com.linbit.linstor.modularcrypto.JclCryptoProvider");
            ModularCryptoProvider jclCryptoProvider = (ModularCryptoProvider) jclCryptoProviderCls.newInstance();

            AbsMigration.setModularCryptoProvider(jclCryptoProvider);
        }
    }

    private static String renderGeneratedDatabaseTables(DatabaseConstantsGenerator generator, String gitRoot)
        throws IOException
    {
        String genDbTablesJavaCode = generator.renderSqlConsts(
            DatabaseConstantsGenerator.DFLT_DBDRIVERS_PACKAGE,
            DatabaseConstantsGenerator.DFLT_GEN_DB_TABLES_CLASS_NAME
        );
        renderJavaFile(
            genDbTablesJavaCode,
            DatabaseConstantsGenerator.DFLT_DBDRIVERS_PACKAGE,
            DatabaseConstantsGenerator.DFLT_GEN_DB_TABLES_CLASS_NAME,
            gitRoot,
            "server"
        );
        return genDbTablesJavaCode;
    }

    private static void renderGeneratedKubernetesCustomResourceDefinitions(
        DatabaseConstantsGenerator generator,
        String gitRoot,
        String genDbTablesJavaCodeRef
    )
        throws IOException
    {
        String crdVersion = loadCurrentGitTag(gitRoot);
        GeneratedCrdResult result = generator.renderKubernetesCustomResourceDefinitions(
            DatabaseConstantsGenerator.DFLT_K8S_CRD_PACKAGE,
            DatabaseConstantsGenerator.DFLT_K8S_CRD_CLASS_NAME_FORMAT,
            crdVersion,
            loadAllCrdVersionNumbers(
                gitRoot,
                DatabaseConstantsGenerator.getYamlLocation("", "")
            ),
            genDbTablesJavaCodeRef
        );

        if (genCrdCurrentChanged(gitRoot, result))
        {
            for (GeneratedCrdJavaClass generatedCrdJavaClass : result.javaClasses)
            {
                renderJavaFile(
                    generatedCrdJavaClass.javaCode,
                    generatedCrdJavaClass.pkgName,
                    generatedCrdJavaClass.clazzName,
                    gitRoot,
                    "server"
                );
            }

            for (GeneratedResources generatedResources : result.resources)
            {
                renderResourceFile(gitRoot, generatedResources.yamlLocation, generatedResources.content);
            }
        }
    }

    private static String loadCurrentGitTag(String gitRoot)
    {
        String ret = null;

        ProcessBuilder pb = new ProcessBuilder("git describe --tags --abbrev=0".split(" "));
        pb.directory(Paths.get(gitRoot).toFile());
        try
        {
            Process process = pb.start();
            int ec = process.waitFor();
            if (ec != 0)
            {
                throw new ImplementationError("'" + pb.command() + "' returned with exit code: " + ec);
            }
            ret = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
            )
                .lines()
                .collect(Collectors.joining("\n"))
                .replaceAll("[.-]", "_");
        }
        catch (IOException | InterruptedException exc)
        {
            throw new ImplementationError("Command failed: " + pb.command(), exc);
        }
        return ret;
    }

    private static boolean genCrdCurrentChanged(String gitRoot, GeneratedCrdResult result)
    {
        boolean ret = true;
        // find GenCrdCurrent
        for (GeneratedCrdJavaClass generatedCrdJavaClass : result.javaClasses)
        {
            if (generatedCrdJavaClass.clazzName.equals(DatabaseConstantsGenerator.DFLT_K8S_CRD_CLASS_NAME_CURRENT))
            {
                Path path = buildJavaPath(
                    gitRoot,
                    "server",
                    generatedCrdJavaClass.pkgName
                )
                    .resolve(generatedCrdJavaClass.clazzName + ".java");

                if (Files.exists(path))
                {
                    String oldCode;
                    try
                    {
                        oldCode = new String(Files.readAllBytes(path));
                    }
                    catch (IOException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    String sanitizedOldCode = replaceVersionUID(oldCode);
                    String sanitizedNewCode = replaceVersionUID(generatedCrdJavaClass.javaCode);

                    ret = !sanitizedNewCode.equals(sanitizedOldCode);
                }
                break;
            }
        }
        return ret;
    }

    private static void printDiff(String sanitizedOldCode, String sanitizedNewCode)
    {
        String[] oldLines = sanitizedOldCode.split("\n");
        String[] newLines = sanitizedNewCode.split("\n");
        String[] rest;
        String restType;
        int minLines;
        int maxLines;
        if (oldLines.length > newLines.length)
        {
            minLines = newLines.length;
            maxLines = oldLines.length;
            rest = oldLines;
            restType = "old";
        }
        else
        {
            minLines = oldLines.length;
            maxLines = newLines.length;
            rest = newLines;
            restType = "new";
        }
        int curLine = 0;
        while (curLine < minLines)
        {
            if (!oldLines[curLine].equals(newLines[curLine]))
            {
                System.out.println("line: " + curLine);
                System.out.println("  old: " + oldLines[curLine]);
                System.out.println("  new: " + newLines[curLine]);
            }
            curLine++;
        }
        while (curLine < maxLines)
        {
            System.out.println("line: " + curLine);
            System.out.println("  " + restType + ": " + rest[curLine]);
            curLine++;
        }
    }

    private static String replaceVersionUID(String javaCode)
    {
        String ret = javaCode.replaceAll(
            "private static final long serialVersionUID = -?\\d+L;",
            "private static final long serialVersionUID = 0L;"
        );

        Pattern pattern = Pattern.compile("public static final String VERSION = \"([^\"]+)\";");
        Matcher matcher = pattern.matcher(ret);
        if (!matcher.find())
        {
            throw new ImplementationError("No version found");
        }
        String version = matcher.group(1);
        version = version.replaceAll("[-_]", "[-_]");

        ret = ret.replaceAll(version, "v0");

        return ret;
    }

    private static Set<String> loadAllCrdVersionNumbers(String gitRoot, String yamlLocationRef) throws IOException
    {
        Path path = Paths.get(gitRoot, "controller", "generated-resources").resolve(Paths.get(".", yamlLocationRef))
            .normalize().getParent();
        Set<String> ret;
        if (Files.isDirectory(path))
        {
            ret = Files.list(path)
                .map(pathtmp -> pathtmp.getFileName().toString())
                .collect(Collectors.toSet());
        }
        else
        {
            ret = new HashSet<>();
        }
        return ret;
    }

    private static void renderJavaFile(
        String javaClazzContentRef,
        String pkgNameRef,
        String javaClazzNameRef,
        String gitRoot,
        String prj
    )
        throws IOException
    {
        Path path = buildJavaPath(gitRoot, prj, pkgNameRef).resolve(javaClazzNameRef + ".java");
        Files.createDirectories(path.getParent()); // ensure parent folder exists
        Files.write(path, javaClazzContentRef.getBytes());
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private static void renderResourceFile(
        String gitRoot,
        String relativePath,
        String content
    )
        throws IOException
    {
        Path path = Paths.get(gitRoot).resolve("controller/generated-resources").resolve(Paths.get(".", relativePath))
            .normalize();
        // getParent can return null, but since we got the path by resolving it just previously the sb-warning is
        // unnecessary
        Files.createDirectories(path.getParent()); // ensure parent folder exists
        Files.write(path, content.getBytes());
    }

    private static Path buildJavaPath(String gitRootRef, String prj, String pkgNameRef)
    {
        return Paths.get(
            gitRootRef,
            prj,
            "generated-dbdrivers",
            pkgNameRef.replaceAll("[.]", "/")
        );
    }

    private static String getGitRoot() throws IOException, InterruptedException
    {
        ProcessBuilder pb = new ProcessBuilder("git", "rev-parse", "--show-toplevel");
        Process proc = pb.start();
        int rc = proc.waitFor();
        if (rc != 0)
        {
            throw new RuntimeException("git base path checking failed:" + pb.toString());
        }
        return new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    }

}
