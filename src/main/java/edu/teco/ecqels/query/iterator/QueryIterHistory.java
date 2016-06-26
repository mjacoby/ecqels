/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.query.iterator;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterProcessBinding;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class QueryIterHistory extends QueryIterProcessBinding{

    public QueryIterHistory(QueryIterator qIter, ExecutionContext context) {
        super(qIter, context);
    }

    @Override
    public Binding accept(Binding binding) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
