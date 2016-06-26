/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.query.iterator;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterFilterExpr;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIteratorBase;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.openjena.atlas.io.IndentedWriter;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class QueryIteratorCopy extends QueryIter {

    List<Binding> elements = new ArrayList<>();
    QueryIterator iterator;

    public QueryIteratorCopy(QueryIterator qIter, ExecutionContext execContext) {
        super(execContext);
        synchronized (this) {
            for (; qIter.hasNext();) {
                elements.add(qIter.nextBinding());
            }
            qIter.close();
            iterator = copy();
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        return iterator.nextBinding();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.print("QueryIteratorCopy");
        out.incIndent();
        out.decIndent();
    }

    public List<Binding> elements() {
        return Collections.unmodifiableList(elements);
    }

    public QueryIterator copy() {
        return new QueryIterPlainWrapper(elements.iterator());
    }

    @Override
    protected void closeIterator() {
        iterator.close();
    }

    @Override
    protected void requestCancel() {
        iterator.cancel();
    }

    @Override
    protected boolean hasNextBinding() {
        return iterator.hasNext();
    }
}
