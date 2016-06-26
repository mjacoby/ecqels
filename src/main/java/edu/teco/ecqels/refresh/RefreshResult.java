/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.refresh;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.engine.QueryIterator;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class RefreshResult {
    private final Op op;
    private final QueryIterator result;
    
    public RefreshResult(Op op, QueryIterator result) {
        this.op = op;
        this.result = result;
    }
    
    public Op getOp() {
        return op;
    }
    
    public QueryIterator getResult() {
        return result;
    }
}
