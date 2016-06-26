/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.nodetable.NodeTableNative;
import com.hp.hpl.jena.tdb.solver.OpExecutorTDB;
import com.hp.hpl.jena.tdb.store.NodeId;
import com.hp.hpl.jena.tdb.sys.SystemTDB;
import com.hp.hpl.jena.util.FileManager;
import edu.teco.ecqels.continuous.ContinuousConstruct;
import edu.teco.ecqels.continuous.ContinuousQuery;
import edu.teco.ecqels.continuous.ContinuousSelect;
import edu.teco.ecqels.lang.parser.ParserECQELS;
import edu.teco.ecqels.query.execution.QueryExecutor;
import edu.teco.ecqels.data.EnQuad;
import edu.teco.ecqels.window.Window;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class Engine {

    private static final Logger logger = LogManager.getLogger();
    private final EPServiceProvider provider;
    private final NodeTable dictionary;
    private final Dataset dataset;
    private final ExecutionContext arqExecutionContext;
    private final ObjectFile dictionaryFileCache;
    private final Map<ContinuousQuery<?>, QueryExecutor> registeredQueries = new HashMap<>();
    private final ExecutorService executorPool;
    private static final ThreadGroup group = new ThreadGroup("ECQELS execution threads");

    public Engine() {
        Configuration esperConfig = new Configuration();
        esperConfig.getEngineDefaults().getThreading().setListenerDispatchTimeout(60000);
        provider = EPServiceProviderManager.getDefaultProvider(esperConfig);
        dictionaryFileCache = FileFactory.createObjectFileMem("temp");
        dictionary = new NodeTableNative(IndexBuilder.mem().newIndex(FileSet.mem(), SystemTDB.nodeRecordFactory), dictionaryFileCache);
        this.dataset = DatasetFactory.createMemFixed();
        this.arqExecutionContext = new ExecutionContext(dataset.getContext(), dataset.asDatasetGraph().getDefaultGraph(), dataset.asDatasetGraph(), OpExecutorTDB.OpExecFactoryTDB);
        this.executorPool = Executors.newCachedThreadPool(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group, r);
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void shutdown() {
        try {
            provider.destroy();
            registeredQueries.values().forEach(q -> q.stop());
            dataset.close();
            dictionaryFileCache.close();
            dictionary.close();
        } catch (Exception e) {
            logger.log(Level.DEBUG, e);
        }
    }

    public ExecutionContext getARQExecutionContext() {
        return arqExecutionContext;
    }

    public void addRDF(String graphUri, String data, String language) {
        addRDF(dataset.getNamedModel(graphUri), data, language);
    }

    public void addRDF(String data, String language) {
        addRDF(dataset.getDefaultModel(), data, language);
    }

    public void deleteRDF(String graphUri, String data, String language) {
        deleteRDF(dataset.getNamedModel(graphUri), data, language);
    }

    public void deleteRDF(String data, String language) {
        deleteRDF(dataset.getDefaultModel(), data, language);
    }

    public Dataset getDataset() {
        return dataset;
    }

    public Query parse(String query) {
        Query result = new Query();
        ParserECQELS parser = new ParserECQELS();
        parser.parse(result, query);
        return result;
    }

    public ContinuousSelect registerSelect(Query query, Map<String, String> variableBindings) {
        QueryExecutor queryExecutor = new QueryExecutor(this, query, variableBindings);
        ContinuousSelect result = new ContinuousSelect(query, getARQExecutionContext());
        queryExecutor.addNewQueryResultAvailableListener(result);
        registeredQueries.put(result, queryExecutor);
        queryExecutor.start();
        return result;
    }

    public void unregisterSelect(ContinuousSelect query) {
        unregister(query);
    }

    public void unregisterConstruct(ContinuousConstruct query) {
        unregister(query);
    }

    public ContinuousConstruct registerConstruct(Query query, Map<String, String> variableBindings) {
        QueryExecutor queryExecutor = new QueryExecutor(this, query, variableBindings);
        ContinuousConstruct result = new ContinuousConstruct(query, getARQExecutionContext());
        queryExecutor.addNewQueryResultAvailableListener(result);
        registeredQueries.put(result, queryExecutor);
        queryExecutor.start();
        return result;
    }

    public void sendAsync(final Node graph, final Node s, final Node p, final Node o) {
        logger.debug("data received on stream " + graph + ": " + s + " " + p + " " + o);
        List<Callable<Void>> tasks = new ArrayList<>(registeredQueries.size());
        for (QueryExecutor query : registeredQueries.values()) {
            executorPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    query.send(graph, s, p, o);
                    return null;
                }
            });
        }

    }

    public void send(Node graph, Node s, Node p, Node o) {
        logger.debug("data received on stream " + graph + ": " + s + " " + p + " " + o);
        if (registeredQueries.size() > 1) {
            List<Callable<Void>> tasks = new ArrayList<>(registeredQueries.size());
            for (QueryExecutor query : registeredQueries.values()) {
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        query.send(graph, s, p, o);
                        return null;
                    }
                });

            }
            try {
                executorPool.invokeAll(tasks);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(Engine.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
        } else {
            registeredQueries.values().forEach(q -> q.send(graph, s, p, o));
        }
    }

    public EPStatement addWindow(Node node, String window) {
        return this.provider.getEPAdministrator().createEPL("select * from edu.teco.ecqels.data.EnQuad" + matchPattern(node) + window);
    }

    private String matchPattern(Node node) {
        if (!node.isVariable()) {
            return "(GID=" + dictionary.getAllocateNodeId(node).getId() + ")";
        }
        return "";
    }

    public Quad decode(EnQuad enQuad) {
        return new Quad(
                decode(enQuad.getGID()),
                enQuad.getSubject(),
                enQuad.getPrdeicate(),
                enQuad.getObject());
    }

    private long encode(Node node) {
        return dictionary.getAllocateNodeId(node).getId();
    }

    private Node decode(Long id) {
        try {
            return dictionary.getNodeForNodeId(NodeId.create(id));
        } catch (Exception e) {
            String t = "";
        }
        return Node.ANY;
    }

    private void unregister(ContinuousQuery query) {
        if (!registeredQueries.containsKey(query)) {
            return;
        }
        registeredQueries.get(query).stop();
        registeredQueries.remove(query);
    }

    private void addRDF(Model model, String data, String language) {
        if (data.isEmpty()) {
            return;
        }
        Model temp = ModelFactory.createDefaultModel();
        temp.read(new ByteArrayInputStream(data.getBytes()), null, language);
        model.add(temp);
    }

    private void deleteRDF(Model model, String data, String language) {
        Model temp = ModelFactory.createDefaultModel();
        temp.read(new ByteArrayInputStream(data.getBytes()), null, language);
        model.remove(temp);
    }
}
