/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.lang.window.WindowInfo;
import edu.teco.ecqels.refresh.RefreshManager;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class WindowFactory {
   
    public static Window createWindow(Engine engine, RefreshManager refreshManager, Node streamNode, BasicPattern pattern, WindowInfo info) {
        switch (info.getType()) {
            case NOW:
                return new Now(engine, streamNode, pattern);
            case ALL:
                return new All(engine, streamNode, pattern);
            case TRIPLES:
                return new TripleWindow(engine, streamNode, pattern, info.getTriples());
            case TUMBLING:
                return new TumblingWindow(engine, streamNode, pattern, info.getSize());
            case SLIDING:
                return (info.getSlide() == null || info.getSlide().inNanoSec() <= 0)
                        ? new TimeEvictedWindow(engine, streamNode, pattern, info.getSize())
                        //? new SlidingWindow(engine, streamNode, pattern, info.getSize(), info.getSize())
                        : new SlidingWindow(engine, streamNode, pattern, info.getSize(), info.getSlide());
            default:
                throw new IllegalStateException();
        }        
    }

    private WindowFactory() {

    }
}
