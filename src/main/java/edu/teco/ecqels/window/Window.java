package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import edu.teco.ecqels.event.DataChangedListener;

public interface Window {

    public void add(final Quad quad);
    public void stop();
    public DatasetGraph getDatasetGraph();    
    public void addDataChangedListener(DataChangedListener listener);
    public void removeDataChangedListener(DataChangedListener listener);
    public QueryIterator evaluate(QueryIterator input, ExecutionContext execCxt);
    public Node getStreamNode();
    public BasicPattern getPattern();
    @Override
    public boolean equals(Object obj);
    
    public Window clone();
}
