/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import edu.teco.ecqels.Engine;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public abstract class ScheduledRefreshableWindow extends AbstractWindow {

    public ScheduledRefreshableWindow(Engine engine, Node streamNode, BasicPattern pattern) {
        super(engine, streamNode, pattern);
    }

    @Override
    public QueryIterator evaluate(QueryIterator input, ExecutionContext execCxt) {        
        purgeBeforeExecution();
        QueryIterator result = super.evaluate(input, execCxt);
        purgeAfterExecution();
        return result;
    }

    protected void purgeBeforeExecution() {

    }

    protected void purgeAfterExecution() {

    }

    public abstract long getRefreshInterval();

}
