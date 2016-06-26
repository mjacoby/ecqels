/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.stream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterConcat;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterNullIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import com.hp.hpl.jena.sparql.engine.main.QC;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.event.DataChangedEvent;
import edu.teco.ecqels.event.DataChangedListener;
import edu.teco.ecqels.event.RefreshRequestedEvent;
import edu.teco.ecqels.event.RefreshRequestedListener;
import edu.teco.ecqels.lang.op.OpStream;
import edu.teco.ecqels.lang.window.WindowInfo;
import edu.teco.ecqels.query.execution.QueryExecutionContext;
import edu.teco.ecqels.query.execution.QueryExecutor;
import edu.teco.ecqels.query.iterator.QueryIteratorCopy;
import edu.teco.ecqels.refresh.RefreshManager;
import edu.teco.ecqels.refresh.RefreshRequest;
import edu.teco.ecqels.util.Utils;
import edu.teco.ecqels.window.ScheduledRefreshableWindow;
import edu.teco.ecqels.window.Window;
import edu.teco.ecqels.window.WindowFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.swing.event.EventListenerList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class StreamExecutor {

    private static final Logger logger = LogManager.getLogger();

    private Engine engine;
    private ExecutionContext executionContext;
    private List<OpStream> ops;
    private EventListenerList listeners = new EventListenerList();
    private final QueryExecutionContext queryContext;
    private final RefreshManager refreshManager;
    private final List<StreamRuntimeInfo> streams = new ArrayList<>();
    private final WindowInfo windowInfo;
    private final Node streamNode;
    private final Op subOp;
    private final BasicPattern pattern;
    private final QueryExecutor queryExecutor;

    public StreamExecutor(Engine engine, ExecutionContext executionContext, QueryExecutor queryExecutor, RefreshManager refreshManager, List<OpStream> ops) {
        if (!executionContext.getContext().isDefined(QueryExecutionContext.SYMBOL)) {
            throw new IllegalArgumentException("executionContext msut contain QueryExecutionContext");
        }
        if (ops == null || ops.isEmpty()) {
            throw new IllegalArgumentException("ops must be non-null and non-empty");
        }
        queryContext = (QueryExecutionContext) executionContext.getContext().get(QueryExecutionContext.SYMBOL);
        this.queryExecutor = queryExecutor;
        this.engine = engine;
        this.executionContext = executionContext;
        this.refreshManager = refreshManager;
        this.ops = ops;
        windowInfo = ops.get(0).getWindowInfo();
        streamNode = ops.get(0).getNode();
        subOp = ops.get(0).getSubOp();
        pattern = ops.get(0).getPattern();
        init();
    }

    protected void init() {
//        findTriples();
        refreshManager.addRefreshRequestedListener(new RefreshRequestedListener() {

            @Override
            public void refreshRequested(RefreshRequestedEvent e) {
                Set<RefreshRequest.RefreshRequestSource> uriRefreshRequestes = e.getRefreshRequest().getSources().stream().filter(source -> Utils.isSubOp(source.getOp(), subOp)).collect(Collectors.toSet());
                if (uriRefreshRequestes == null || uriRefreshRequestes.isEmpty()) {
                    return;
                }
                if (uriRefreshRequestes.size() > 1) {
                    throw new IllegalStateException("should never happen");
                }
                queryContext.setRefreshedOps(Arrays.asList(new Op[]{uriRefreshRequestes.iterator().next().getOp()}));
                refreshBindedStreams();
                queryContext.clearRefreshedOps();
            }
        });
    }

    private void refreshBindedStreams() {
        List<String> uri = new ArrayList<>();
        uri.add(Var.alloc(streamNode).toString());
        ResultSet result = ResultSetFactory.create(QC.execute(subOp, BindingFactory.root(), executionContext), uri);
        // here would be the right point to not only check internal meta data repository but also remote one
        // this could be done via encapsulating subOp inside an OpService instance which would automatically redirect the query to the server

        List<Node> currentStreams = new ArrayList<>();
        while (result.hasNext()) {
            currentStreams.add(result.next().get(streamNode.toString()).asNode());
        }
        // handle added streams, i.e. entries in currentStreams but not in windows
        List<Node> temp = new ArrayList<>(currentStreams);
        temp.removeAll(streams.stream().map(s -> s.getWindow().getStreamNode()).collect(Collectors.toList()));
        for (Node added : temp) {
            addStream(added);
        }
        // handle deleted streams, i.e. entries in windows that are not in currentStreams
        temp = new ArrayList<>(streams.stream().map(s -> s.getWindow().getStreamNode()).collect(Collectors.toList()));
        temp.removeAll(currentStreams);
        for (Node deleted : temp) {
            removeStream(deleted);
            // unsubscribe esper
            // delete window? or keep it till next evaluation?
        }
    }

    private RefreshRequest combineAllResultsWithNew() {
        RefreshRequest result = new RefreshRequest();
        QueryIterConcat queryResult = new QueryIterConcat(executionContext);
        streams.stream().forEach(s -> queryResult.add(s.getCurrentResult()));
        ops.stream().forEach((op) -> {
            result.addSource(op, queryResult);
        });
        return result;
    }

    protected void removeStream(Node streamNode) {
        StreamRuntimeInfo stream = streams.stream().filter(s -> s.getWindow().getStreamNode().equals(streamNode)).findFirst().get();
        queryExecutor.unregisterFromStream(streamNode, this);
        if (stream.isScheduledRefreshable()) {
            // unschedule task
            // keep in memory till next schedule so that partially existing windows get evaluated
        } else {
            // remove listener
            // immediately remove data
            stream.getWindow().stop();
            streams.remove(stream);
        }
    }

    public RefreshRequest send(Node graph, Node s, Node p, Node o) {
        boolean refreshed = false;
        for (StreamRuntimeInfo info : streams) {
            if (info.getWindow().getStreamNode().equals(graph)) {
                info.getWindow().add(new Quad(graph, s, p, o));
                if (!info.scheduledRefreshable) {
                    info.setCurrentResult(info.getWindow().evaluate(QueryIterRoot.create(executionContext), executionContext));
                    refreshed = true;
                }
            }
        }
        if (refreshed) {
            return combineAllResultsWithNew();
        }
        return RefreshRequest.empty();
    }

    protected void addStream(Node streamNode) {
        queryExecutor.registerToStream(streamNode, this);
        StreamRuntimeInfo stream = new StreamRuntimeInfo(WindowFactory.createWindow(engine, refreshManager, streamNode, pattern, windowInfo));
        streams.add(stream);
        if (stream.isScheduledRefreshable()) {
            // nothing to do here, all done in start()
        } else {
            stream.setEventHandler(new DataChangedListener() {

                @Override
                public void dataChanged(DataChangedEvent e) {
                    // materialize
                    stream.setCurrentResult(e.getResult());
                    fireRefreshRequestedListener(new RefreshRequestedEvent(StreamExecutor.this, combineAllResultsWithNew()));
                }
            });
            stream.getWindow().addDataChangedListener(stream.getEventHandler());
        }
    }

    public void start() {
        if (windowInfo.isScheduledRefreshable()) {
            refreshManager.schedule(new Callable<RefreshRequest>() {

                @Override
                public RefreshRequest call() throws Exception {
                    // reevaluate all streams and notify result
                    RefreshRequest result = new RefreshRequest();
                    QueryIterConcat queryResult = new QueryIterConcat(executionContext);
                    for (StreamRuntimeInfo stream : streams) {
                        queryResult.add(((ScheduledRefreshableWindow) stream.getWindow()).evaluate());
                    }
                    ops.stream().forEach((op) -> {
                        result.addSource(op, queryResult);
                    });
                    return result;
                }
            }, windowInfo.getScheduleInterval().inMiliSec());
        }
        if (streamNode.isVariable()) {
            refreshBindedStreams();
            // this means streams can be added/deleted
            // do initial execution of subOp to find machting streams
        } else {
            // we have a static stream, so just register it and be happy
            addStream(streamNode);
        }
    }

    public void stop() {
        streams.forEach(s -> s.getWindow().stop());
    }

//    protected void findTriples() {
//        OpWalker.walk(op.getPattern(), new OpVisitorBase() {
//            @Override
//            public void visit(OpTriple opTriple) {
//                triples.add(opTriple.getTriple());
//            }
//
//            @Override
//            public void visit(OpBGP opBGP) {
//                triples.addAll(opBGP.getPattern().getList());
//            }
//
//        });
//    }
    public void addRefreshRequestedListener(RefreshRequestedListener listener) {
        listeners.add(RefreshRequestedListener.class, listener);
    }

    public void removeRefreshRequestedListener(RefreshRequestedListener listener) {
        listeners.remove(RefreshRequestedListener.class, listener);
    }

    protected void fireRefreshRequestedListener(RefreshRequestedEvent e) {
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == RefreshRequestedListener.class) {
                ((RefreshRequestedListener) temp[i + 1]).refreshRequested(e);
            }
        }
    }

//    public Op getOp() {
//        return op;
//    }
    private class StreamRuntimeInfo {

        private final Window window;
        private final boolean scheduledRefreshable;
        private Callable<RefreshRequest> scheduledHandler;
        private DataChangedListener eventHandler;
        private QueryIteratorCopy currentResult;

        public StreamRuntimeInfo(Window window) {
            this.window = window;
            scheduledRefreshable = window instanceof ScheduledRefreshableWindow;
            currentResult = new QueryIteratorCopy(new QueryIterNullIterator(executionContext), executionContext);//QueryIteratorCopy(QueryIterRoot.create(executionContext));
        }

        /**
         * @return the window
         */
        public Window getWindow() {
            return window;
        }

        /**
         * @return the scheduledRefreshable
         */
        public boolean isScheduledRefreshable() {
            return scheduledRefreshable;
        }

        /**
         * @return the scheduledHandler
         */
        public Callable<RefreshRequest> getScheduledHandler() {
            return scheduledHandler;
        }

        /**
         * @param scheduledHandler the scheduledHandler to set
         */
        public void setScheduledHandler(Callable<RefreshRequest> scheduledHandler) {
            this.scheduledHandler = scheduledHandler;
        }

        /**
         * @return the eventHandler
         */
        public DataChangedListener getEventHandler() {
            return eventHandler;
        }

        /**
         * @param eventHandler the eventHandler to set
         */
        public void setEventHandler(DataChangedListener eventHandler) {
            this.eventHandler = eventHandler;
        }

        /**
         * @return the currentResult
         */
        public QueryIterator getCurrentResult() {
            return currentResult.copy();
        }

        /**
         * @param currentResult the currentResult to set
         */
        public void setCurrentResult(QueryIterator currentResult) {
            if (this.currentResult != null) {
                this.currentResult.close();
            }
            this.currentResult = new QueryIteratorCopy(currentResult, executionContext);
            this.currentResult.close();
        }
    }
}
