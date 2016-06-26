/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import edu.teco.ecqels.Engine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import edu.teco.ecqels.lang.window.Duration;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class TumblingWindow extends ScheduledRefreshableWindow {

    private static final Logger logger = LogManager.getLogger();
    protected final Duration size;

    public TumblingWindow(Engine engine, Node streamNode, BasicPattern pattern, Duration size) {
        super(engine, streamNode, pattern);
        if (size == null || size.inNanoSec() <= 0) {
            throw new IllegalArgumentException("window size must be non-null and > 0");
        }
        this.size = size;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public long getRefreshInterval() {
        return size.inMiliSec();
    }

    @Override
    public void purgeAfterExecution() {
        datasetGraph.deleteAny(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TumblingWindow)) {
            return false;
        }
        TumblingWindow window = (TumblingWindow) obj;
        return window.size.equals(size);
    }

    @Override
    public Window clone() {
        return new TumblingWindow(engine, streamNode, pattern, size);
    }
}
