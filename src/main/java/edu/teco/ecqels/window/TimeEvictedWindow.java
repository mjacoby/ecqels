/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterNullIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import edu.teco.ecqels.Engine;
import java.util.Timer;
import java.util.TimerTask;
import edu.teco.ecqels.lang.window.Duration;
import edu.teco.ecqels.query.iterator.QueryIteratorCopy;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class TimeEvictedWindow extends AbstractWindow {

    protected final Duration size;
    protected Timer timer;

    public TimeEvictedWindow(Engine engine, Node streamNode, BasicPattern pattern, Duration size) {
        super(engine, streamNode, pattern);
        this.size = size;
        timer = new Timer("window eviction timer", true);
    }

    @Override
    public void add(final Quad quad) {
        if (timer != null) {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {

                    try {                       
                        datasetGraph.getLock().enterCriticalSection(false);
                        datasetGraph.delete(quad);
                        datasetGraph.getLock().leaveCriticalSection();
                        
                        QueryIterator temp = evaluate();
                        
                        datasetGraph.getLock().enterCriticalSection(true);
                        QueryIteratorCopy result = new QueryIteratorCopy(temp, engine.getARQExecutionContext());
                        result.close();
                        datasetGraph.getLock().leaveCriticalSection();
                        
                        //fireDataChanged(result.copy());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, size.inMiliSec());
            super.add(quad);
        }
    }

    @Override
    public void stop() {
        super.stop();
        timer.cancel();
        timer = null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TimeEvictedWindow)) {
            return false;
        }
        TimeEvictedWindow window = (TimeEvictedWindow) obj;
        return window.size.equals(size);
    }

    @Override
    public Window clone() {
        return new TimeEvictedWindow(engine, streamNode, pattern, size);
    }

}
