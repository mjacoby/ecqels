/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.refresh;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import edu.teco.ecqels.refresh.RefreshRequest.RefreshRequestSource;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class RefreshRequest {

    public static RefreshRequest empty() {
        return new RefreshRequest();
    }
    
    private final List<RefreshRequestSource> sources;
    
    public RefreshRequest() {
        sources = new ArrayList<>();
    }
    
    public RefreshRequest(Op op, QueryIterator result) {
        this();
        addSource(op, result);
    }
    
    public void addSource(Op op, QueryIterator result) {
        sources.add(new RefreshRequestSource(op, result));
    }
    
    public void addSource(RefreshRequest request) {
        sources.addAll(request.getSources());
    }
    
    public List<RefreshRequestSource> getSources() {
        return sources;
    }
    
    public class RefreshRequestSource {

        private final Op op;
        private final QueryIterator result;

        public RefreshRequestSource(Op op, QueryIterator result) {
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
}
