/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import edu.teco.ecqels.query.iterator.QueryIteratorCopy;
import edu.teco.ecqels.event.NewQueryResultAvailableEvent;
import java.util.ArrayList;
import java.util.List;
import javax.swing.event.EventListenerList;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class AbstractContinuousQuery<T extends ContinuousListener> implements ContinuousQuery<T> {

    protected Query query;
    protected List<T> listeners = new ArrayList<>();
    protected final ExecutionContext executionContext;

    public AbstractContinuousQuery(Query query, ExecutionContext executionContext) {        
        this.query = query;
        this.executionContext = executionContext;
    }

    @Override
    public void newQueryResultAvailable(NewQueryResultAvailableEvent e) {
        fireUpdate(e.getResult());
    }

    public void addListener(T listener) {
        listeners.add(listener);
    }

    public void removeListener(T listener) {
        listeners.remove(listener);
    }

    @Override
    public Query getQuery() {
        return query;
    }

    protected void fireUpdate(QueryIterator result) {
        QueryIteratorCopy iteratorCopy = null;
        if (listeners.size() > 1) {
            iteratorCopy = new QueryIteratorCopy(result, executionContext);
        }
        for (T listener : listeners) {
            listener.update(iteratorCopy == null ? result : iteratorCopy.copy());
            //listener.update(QueryIter.materialize(result));
        }
        if (listeners.size() > 1) {
            iteratorCopy.close();
        }
    }
}
