/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.query.execution;

import com.hp.hpl.jena.graph.Node;
import edu.teco.ecqels.algebra.Algebra;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.util.NodeFactory;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.op.CachedOpExecutor;
import edu.teco.ecqels.query.iterator.QueryIteratorCopy;
import edu.teco.ecqels.refresh.RefreshManager;
import edu.teco.ecqels.refresh.RefreshRequest;
import edu.teco.ecqels.refresh.RefreshRequest.RefreshRequestSource;
import edu.teco.ecqels.stream.StreamExecutor;
import edu.teco.ecqels.refresh.UpdateRefreshIntervalsTransform;
import edu.teco.ecqels.event.NewQueryResultAvailableEvent;
import edu.teco.ecqels.event.NewQueryResultAvailableListener;
import edu.teco.ecqels.event.RefreshRequestedEvent;
import edu.teco.ecqels.event.RefreshRequestedListener;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import javax.swing.event.EventListenerList;
import edu.teco.ecqels.lang.op.OpRefreshable;
import edu.teco.ecqels.lang.op.OpRefreshableGraph;
import edu.teco.ecqels.lang.op.OpRefreshableService;
import edu.teco.ecqels.lang.op.OpStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import edu.teco.ecqels.util.Utils;
import edu.teco.ecqels.window.Window;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class QueryExecutor implements RefreshRequestedListener {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();
    public static final Var ABORT_QUERY_VARIABLE = Var.alloc("ABORT_QUERY");
    protected boolean stop = false;
    protected Engine engine;
    protected ExecutionContext executionContext;
    protected QueryExecutionContext queryExecutionContext;
    protected Query query;
    protected Op op;
    protected EventListenerList listeners = new EventListenerList();
    protected final List<StreamExecutor> streams = new ArrayList<>();
    protected ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    protected final BlockingQueue<RefreshRequest> refreshRequestQueue = new LinkedBlockingQueue<>();
    protected List<OpRefreshable> refreshables;
    protected final RefreshManager refreshManager = new RefreshManager();
    protected BindingMap initialBinding = BindingFactory.create();
    private final Map<Node, List<StreamExecutor>> streamRegistrations = new HashMap<>();

    public QueryExecutor(Engine engine, Query query) {
        this(engine, query, new HashMap<>());
    }

    public QueryExecutor(Engine engine, Query query, Map<String, String> variableBindings) {
        this.engine = engine;
        this.query = query;

        //Op opOptimzed = Algebra.optimize(Algebra.compile(query));
        Op opOptimzed = Algebra.compile(query);
        this.op = Transformer.transform(new UpdateRefreshIntervalsTransform(opOptimzed), opOptimzed);
        this.refreshManager.addRefreshRequestedListener(this);
        initVariableBindings(variableBindings);
        init();
    }

    public void registerToStream(Node stream, StreamExecutor executor) {
        if (!streamRegistrations.containsKey(stream)) {
            streamRegistrations.put(stream, new ArrayList<>());
        }
        streamRegistrations.get(stream).add(executor);
    }

    public void unregisterFromStream(Node stream, StreamExecutor executor) {
        if (streamRegistrations.containsKey(stream)) {
            streamRegistrations.get(stream).remove(executor);
        }
    }

    public void send(Node graph, Node s, Node p, Node o) {
        RefreshRequest request = RefreshRequest.empty();
        if (streamRegistrations.containsKey(graph)) {
            for (StreamExecutor stream : streamRegistrations.get(graph)) {
                request.addSource(stream.send(graph, s, p, o));
            }
        }
        if (!request.getSources().isEmpty()) {
            // need to update
            execute(request);
        }
    }

    private void initVariableBindings(Map<String, String> variableBindings) {
        for (Map.Entry<String, String> binding : variableBindings.entrySet()) {
            //check type and parse content of bindinge for URI, literal, etc
            initialBinding.add(Var.alloc(binding.getKey()), NodeFactory.parseNode(binding.getValue()));
        }
    }

    protected final boolean isStop() {
        return stop;
    }

    public void start() {
        startRefreshables();
        startStreams();
        refreshManager.start();
    }

    public void stop() {
        stop = true;
        refreshManager.stop();
        streams.stream().forEach((stream) -> stream.stop());
        executor.shutdownNow();
    }

    private void init() {
        buildContexts();
        buildStreams();
        buildRefreshables();
        executor.execute(() -> {
            while (!isStop()) {
                try {
                    execute(refreshRequestQueue.take());
                } catch (Exception ex) {
                    //Logger.getLogger(QueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });
    }

    protected void buildContexts() {        
        executionContext = new ExecutionContext(new Context(), engine.getARQExecutionContext().getActiveGraph(), engine.getARQExecutionContext().getDataset(), CachedOpExecutor.CQELSOpExecFactory);
        queryExecutionContext = new QueryExecutionContext(op, executionContext);
        executionContext.getContext().set(QueryExecutionContext.SYMBOL, queryExecutionContext);
    }

    protected void buildStreams() {
        List<OpStream> streamOpsTemp = Utils.findInstacesOf(op, OpStream.class);
        List<List<OpStream>> streamOps = new ArrayList<>();
        // group stream by same node and window AND subops
        for (OpStream stream : streamOpsTemp) {
            boolean isAdded = false;
            for (List<OpStream> streams : streamOps) {
                if (!streams.isEmpty()) {
                    if (streams.get(0).equalTo(stream, new NodeIsomorphismMap())) {
                        streams.add(stream);
                        isAdded = true;
                        break;
                    }
                }
            }
            if (!isAdded) {
                List<OpStream> newList = new ArrayList<>();
                newList.add(stream);
                streamOps.add(newList);
            }
        }
        for (List<OpStream> streamOp : streamOps) {
            StreamExecutor stream = new StreamExecutor(engine, executionContext, this, refreshManager, streamOp);
            stream.addRefreshRequestedListener(this);
            streams.add(stream);
        }
    }

    protected void buildRefreshables() {
        refreshables = Utils.findInstacesOf(op, OpRefreshableGraph.class);
        refreshables.addAll(Utils.<OpRefreshable, OpRefreshableService>findInstacesOf(op, OpRefreshableService.class));
    }

    protected void startStreams() {
        for (StreamExecutor stream : streams) {
            stream.start();
        }
    }

    protected void startRefreshables() {
        for (OpRefreshable refreshable : refreshables) {
            refreshManager.schedule(new Callable<RefreshRequest>() {

                @Override
                public RefreshRequest call() throws Exception {
                    return new RefreshRequest((Op) refreshable, QC.execute((Op) refreshable, CachedOpExecutor.createRootQueryIterator(executionContext), executionContext));
                }
            }, refreshable.getDuration().inMiliSec());
        }
    }

    protected synchronized void execute(RefreshRequest refreshRequest) {
        executionContext.getDataset().getLock().enterCriticalSection(true);
        List<Op> refreshedOps = new ArrayList<>();
        for (RefreshRequestSource source : refreshRequest.getSources()) {
            queryExecutionContext.getCache().put(source.getOp(), source.getResult());
            refreshedOps.add(source.getOp());
        }
        queryExecutionContext.setRefreshedOps(refreshedOps);
        QueryIterator result = QC.execute(op, initialBinding, executionContext);
        queryExecutionContext.clearRefreshedOps();
        // check if result it is not a dummy result
        processResult(result, queryExecutionContext);
        executionContext.getDataset().getLock().leaveCriticalSection();
    }

    private void processResult(QueryIterator result, QueryExecutionContext queryExecutionContext) {
        QueryIteratorCopy resultAsCopy;
        if (result instanceof QueryIteratorCopy) {
            resultAsCopy = (QueryIteratorCopy) result;
        } else {
            resultAsCopy = new QueryIteratorCopy(result, executionContext);
        }
        List<Binding> tempResult = new ArrayList<>();
        while (resultAsCopy.hasNext()) {
            Binding currentBinding = resultAsCopy.nextBinding();
            if (currentBinding.isEmpty()) {
                continue;
            };
            if (currentBinding.contains(ABORT_QUERY_VARIABLE)) {
                if (currentBinding.get(ABORT_QUERY_VARIABLE).getLiteralValue() == NodeValue.TRUE) {
                    continue;
                }
            }
            tempResult.add(currentBinding);
        }
        result.close();
        resultAsCopy.close();
        if (tempResult.size() > 0) {
            QueryIterator finalResult = new QueryIterPlainWrapper(tempResult.iterator());
            fireNewQueryResultAvailable(new NewQueryResultAvailableEvent(this, finalResult, queryExecutionContext.getRefreshedOps()));
        }
    }

    public void addNewQueryResultAvailableListener(NewQueryResultAvailableListener listener) {
        listeners.add(NewQueryResultAvailableListener.class, listener);
    }

    public void removeNewQueryResultAvailableListener(NewQueryResultAvailableListener listener) {
        listeners.remove(NewQueryResultAvailableListener.class, listener);
    }

    protected void fireNewQueryResultAvailable(NewQueryResultAvailableEvent e) {
        if (e.getResult() == null) {
            return;
        }
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == NewQueryResultAvailableListener.class) {
                ((NewQueryResultAvailableListener) temp[i + 1]).newQueryResultAvailable(e);
            }
        }
    }

    @Override
    public void refreshRequested(RefreshRequestedEvent e) {
        //refreshRequestQueue.add(e.getRefreshRequest());       
        execute(e.getRefreshRequest());
    }

}
