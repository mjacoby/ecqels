/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.lang.op;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import edu.teco.ecqels.query.iterator.QueryIterHistory;
import org.openjena.atlas.io.IndentedWriter;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class OpHistory extends OpExt {

    private Op sub;
    
    public OpHistory(String name) {
        super(name);
    }

    @Override
    public Op effectiveOp() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public QueryIterator eval(QueryIterator input, ExecutionContext execCxt) {
        QueryIterator temp = QC.execute(sub, input, execCxt);
        return new QueryIterHistory(temp, execCxt);
    }

    @Override
    public void outputArgs(IndentedWriter out, SerializationContext sCxt) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
