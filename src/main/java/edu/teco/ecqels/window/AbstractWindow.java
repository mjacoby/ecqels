/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.window;

import com.espertech.esper.client.EPStatement;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.tdb.solver.OpExecutorTDB;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.event.DataChangedEvent;
import edu.teco.ecqels.event.DataChangedListener;
import javax.swing.event.EventListenerList;
import org.apache.logging.log4j.LogManager;
import edu.teco.ecqels.data.EnQuad;
import edu.teco.ecqels.op.CachedOpExecutor;
import edu.teco.ecqels.query.execution.QueryExecutor;
import edu.teco.ecqels.query.iterator.QueryIteratorCopy;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public abstract class AbstractWindow implements Window {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

    protected final DatasetGraph datasetGraph;
    protected final Node streamNode;
    protected final BasicPattern pattern;
    protected final Engine engine;
    protected EventListenerList listeners = new EventListenerList();
    protected boolean stop = false;
    private EPStatement statement;
    protected OpGraph op;

    public AbstractWindow(Engine engine, Node streamNode, BasicPattern pattern) {
        this.engine = engine;
        this.streamNode = streamNode;
        this.pattern = pattern;
        this.datasetGraph = DatasetFactory.createMemFixed().asDatasetGraph();
        //registerWithEsper();
    }

    public QueryIterator evaluate() {
        return evaluate(QueryIterRoot.create(engine.getARQExecutionContext()), engine.getARQExecutionContext());
    }

    @Override
    public QueryIterator evaluate(QueryIterator input, ExecutionContext execCxt) {
        if (op == null) {
            op = new OpGraph(streamNode, new OpBGP(pattern));
        }
        ExecutionContext context = new ExecutionContext(execCxt.getContext(), null, datasetGraph, execCxt.getExecutor());
        //input = QueryIterRoot.create(context);
        //context = new ExecutionContext(datasetGraph.getContext(), datasetGraph.getDefaultGraph(), datasetGraph, OpExecutorTDB.OpExecFactoryTDB);
        //QueryIterator result = null;
        datasetGraph.getLock().enterCriticalSection(true);
        //QueryIterator result = QC.execute(op, input, context);
        QueryIterator result = CachedOpExecutor.OpExecFactoryTDB.create(context).executeOp(op, input);
        datasetGraph.getLock().leaveCriticalSection();
        return result;
    }

    @Override
    public Node getStreamNode() {
        return streamNode;
    }

    @Override
    public BasicPattern getPattern() {
        return pattern;
    }

//    private void registerWithEsper() {        
//        statement = engine.addWindow(streamNode, getEsperWindow());//".win:length(1)");
//        statement.setSubscriber(new Object() {
//            public void update(EnQuad enQuad) {
//                add(engine.decode(enQuad));
//            }
//        });
//    }

//    protected String getEsperWindow() {
//        return ".win:length(1)";
//    }

    public void add(final Quad quad) {
        if (stop) {
            return;
        }
        datasetGraph.getLock().enterCriticalSection(false);
        datasetGraph.add(quad);
        datasetGraph.getLock().leaveCriticalSection();
    }

    @Override
    public DatasetGraph getDatasetGraph() {
        return datasetGraph;
    }

    @Override
    public void addDataChangedListener(DataChangedListener listener) {
        listeners.add(DataChangedListener.class, listener);
    }

    @Override
    public void removeDataChangedListener(DataChangedListener listener) {
        listeners.remove(DataChangedListener.class, listener);
    }

    protected void fireDataChanged(QueryIterator result) {
        if (stop) {
            return;
        }
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == DataChangedListener.class) {
                ((DataChangedListener) temp[i + 1]).dataChanged(new DataChangedEvent(this, this, result));
            }
        }
    }

    @Override
    public void stop() {
        stop = true;
//        if (!statement.isStopped() && !statement.isDestroyed()) {
//            statement.destroy();
//        }
        datasetGraph.close();
    }

    public boolean equals(Object obj) {
        return !(obj == null || !(obj instanceof AbstractWindow));
    }

    public abstract Window clone();

}
