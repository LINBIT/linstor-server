package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

@Singleton
class PreSelector
{
    private final static Path SCRIPT_BASE_PATH = Paths.get("/etc/linstor/selector/");

    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private final SystemConfRepository sysCfgRep;
    private Thread scriptThread;

    @Inject
    PreSelector(
        @SystemContext AccessContext apiCtxRef,
        SystemConfRepository sysCfgRepRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCtx = apiCtxRef;
        sysCfgRep = sysCfgRepRef;
        errorReporter = errorReporterRef;
    }

    Collection<StorPoolWithScore> preselect(
        Collection<StorPoolWithScore> ratingsRef
    )
        throws AccessDeniedException
    {
        Collection<StorPoolWithScore> ret = ratingsRef;

        String preselectorFileName = sysCfgRep.getCtrlConfForView(apiCtx).getProp(
            ApiConsts.KEY_AUTOPLACE_PRE_SELECT_FILE_NAME,
            ApiConsts.NAMESPC_AUTOPLACER
        );
        if (Files.exists(SCRIPT_BASE_PATH) && preselectorFileName != null)
        {
            Path preselectorFile;
            try (Stream<Path> fileList = Files.list(SCRIPT_BASE_PATH))
            {
                preselectorFile = fileList.filter(
                    path -> path.getFileName().toString().equals(preselectorFileName)
                ).findFirst().orElse(null);
            }
            catch (IOException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR,
                        String.format(
                            "An IO exception occured when looking for the preselection file"
                        )
                    )
                );
            }

            if (preselectorFile != null)
            {
                long scriptAllowedRunTime = 5_000; // TODO make configurable

                ScriptEngine jsEngine = initializeJSEngine();
                try
                {
                    jsEngine.eval(new FileReader(preselectorFile.toFile()));
                }
                catch (ScriptException | FileNotFoundException exc)
                {
                    throw new ApiException("Failed to evaluate script '" + preselectorFile.toString() + "'", exc);
                }
                Invocable invocable = (Invocable) jsEngine;
                Runnable scriptRunner = invocable.getInterface(Runnable.class);

                List<StorPoolScriptPojo> scriptPojoList = convertStorPoolList(ratingsRef);

                Bindings bindings = jsEngine.getBindings(ScriptContext.ENGINE_SCOPE);
                bindings.put("storage_pools", scriptPojoList);

                if (scriptThread != null && scriptThread.isAlive())
                {
                    errorReporter.logWarning(
                        "Autoplacer.Preselector: Previously executed script still not terminated. Skipping pre-selection."
                    );
                }
                else
                {
                    scriptThread = new Thread(scriptRunner, "Autoplacer-Preselector-ScriptThread");
                    scriptThread.start();
                    try
                    {
                        scriptThread.join(scriptAllowedRunTime);
                    }
                    catch (InterruptedException exc)
                    {
                        // ignored
                    }

                    if (scriptThread.isAlive())
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_PRE_SELECT_SCRIPT_DID_NOT_TERMINATE,
                                String.format(
                                    "Given script file '%s' executed but did not stop after %d ms. Skipping future pre-selection step.",
                                    preselectorFile.toString(),
                                    scriptAllowedRunTime
                                )
                            )
                        );
                    }
                    scriptThread = null;

                    ret = new ArrayList<>();
                    for (StorPoolScriptPojo spPojo : scriptPojoList)
                    {
                        if (spPojo != null && spPojo.storPool != null)
                        {
                            errorReporter.logTrace(
                                "Autoplacer.Preselector: StorPool '%s' on node '%s' with score %f",
                                spPojo.storPool.getName().displayValue,
                                spPojo.storPool.getNode().getName().displayValue,
                                spPojo.score
                            );
                            // just make sure this entry was not created somehow by the script...
                            ret.add(new StorPoolWithScore(spPojo.storPool, spPojo.score));
                        }
                    }
                }
            }
            else
            {
                errorReporter.logTrace(
                    "Autoplacer.Preselector: Given script file '%s' not found. Skipping pre-selection step",
                    SCRIPT_BASE_PATH + preselectorFileName
                );
            }
        }
        else
        {
            errorReporter.logTrace("Autoplacer.Preselector: No script file given. Skipping pre-selection step");
        }
        return ret;
    }

    /**
     * We need to directly use the Nashorn-internal class ClassFilter in order to prevent
     * arbitrary calls to Java-classes. This should prevent the user from making IO or starting
     * threads and such.
     * Nashorn is deprecated since Java 11, and will most likely be completely removed with Java 15.
     * Starting with Java 11, we could use GraalJS, which also has its own ClassFilter.
     *
     * @return
     */
    @SuppressWarnings("restriction")
    private ScriptEngine initializeJSEngine()
    {
        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
        ClassFilter classFilter = new ClassFilter()
        {
            @Override
            public boolean exposeToScripts(String arg0Ref)
            {
                return false;
            }
        };
        return factory.getScriptEngine(classFilter);
    }

    /**
     * Creates a POJO-wrapper around the given storage pools limiting the access from JS
     *
     * @param ratingsRef
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    private List<StorPoolScriptPojo> convertStorPoolList(
        Collection<StorPoolWithScore> ratingsRef
    )
        throws AccessDeniedException
    {
        List<StorPoolScriptPojo> spPojoList = new ArrayList<>();
        for (StorPoolWithScore spWithScore : ratingsRef)
        {
            StorPoolScriptPojo spPojo = new StorPoolScriptPojo(spWithScore);
            spPojoList.add(spPojo);
        }

        spPojoList.sort((spPojo1, spPojo2) -> Double.compare(spPojo1.score, spPojo2.score));
        return spPojoList;
    }

    public class StorPoolScriptPojo
    {
        private final StorPool storPool;
        public double score; /* intentionally not final */

        /**
         * Lazily initialized variables
         */
        private NodeScriptPojo node;

        public StorPoolScriptPojo(StorPoolWithScore spWithScore)
        {
            storPool = spWithScore.storPool;
            score = spWithScore.score;
        }

        public NodeScriptPojo getNode()
        {
            if (node == null)
            {
                node = new NodeScriptPojo(storPool.getNode());
            }
            return node;
        }

        public String getName()
        {
            return storPool.getName().displayValue;
        }

        public String getProvider_kind()
        {
            return storPool.getDeviceProviderKind().name();
        }

        public Long getFree_capacity() throws AccessDeniedException
        {
            return storPool.getFreeSpaceTracker().getFreeCapacityLastUpdated(apiCtx).orElse(null);
        }

        public Long getTotal_capacity() throws AccessDeniedException
        {
            return storPool.getFreeSpaceTracker().getTotalCapacity(apiCtx).orElse(null);
        }

        public Map<String, String> getProperties() throws AccessDeniedException
        {
            return storPool.getProps(apiCtx).map();
        }
    }

    public class NodeScriptPojo
    {
        private final Node node;

        public NodeScriptPojo(Node nodeRef)
        {
            node = nodeRef;
        }

        public String getName()
        {
            return node.getName().displayValue;
        }

        public Map<String, String> getProperties() throws AccessDeniedException
        {
            return node.getProps(apiCtx).map();
        }
    }
}
