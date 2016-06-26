/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.event;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import java.util.ArrayList;
import java.util.EventObject;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class NewQueryResultAvailableEvent extends EventObject {

    private final QueryIterator result;
    private final List<Op> triggeredBy = new ArrayList<>();

    public NewQueryResultAvailableEvent(Object source, QueryIterator result, Op triggeredBy) {
        super(source);
        this.result = result;
        this.triggeredBy.add(triggeredBy);
    }

    public NewQueryResultAvailableEvent(Object source, QueryIterator result, List<? extends Op> triggeredBy) {
        super(source);
        this.result = result;
        this.triggeredBy.addAll(triggeredBy);
    }

    public QueryIterator getResult() {
        return result;
    }

    public List<Op> getTriggeredBy() {
        return triggeredBy;
    }

}
