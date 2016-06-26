package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.query.iterator.QueryIteratorCopy;
import java.util.Deque;
import java.util.LinkedList;

public class TripleWindow extends AbstractWindow {

    private final Deque<Quad> cache;
    private final long count;

    public TripleWindow(Engine engine, Node streamNode, BasicPattern pattern, long count) {
        super(engine, streamNode, pattern);
        this.count = count;
        cache = new LinkedList<>();
    }

    @Override
    public void add(final Quad quad) {
        super.add(quad);
        cache.addLast(quad);
        if (cache.size() > count) {
            datasetGraph.getLock().enterCriticalSection(false);
            Quad toDelete = cache.pollFirst();
            datasetGraph.delete(toDelete);
            cache.stream().filter(e -> e.equals(toDelete)).forEach(e -> datasetGraph.add(e));
            datasetGraph.getLock().leaveCriticalSection();
        }
//        QueryIterator result = evaluate();
//        QueryIteratorCopy temp = new QueryIteratorCopy(result);
//        fireDataChanged(temp.copy());
    }

    public void stop() {
        super.stop();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TripleWindow)) {
            return false;
        }
        TripleWindow window = (TripleWindow) obj;
        return window.count == count;
    }

    public Window clone() {
        return new TripleWindow(engine, streamNode, pattern, count);
    }
}
