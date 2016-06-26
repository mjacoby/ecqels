/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.lang.op;

import edu.teco.ecqels.op.FilterWithHistoryState;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.sse.writers.WriterOp;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import edu.teco.ecqels.query.iterator.QueryIterFilterWithHistoryExpr;
import org.openjena.atlas.io.IndentedWriter;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class OpFilterWithHistory extends OpExt {

    private final OpFilter opFilter;
    private FilterWithHistoryState state = new FilterWithHistoryState();
    private final Expr expr;
    private final Op op;
    private final String varName;

    public OpFilterWithHistory(Expr expr, Op op, String varName) {
        super("FILTER");
        opFilter = OpFilter.filterDirect(new ExprList(expr), op);
        this.op = op;
        this.expr = expr;
        this.varName = varName;
    }

    @Override
    public Op effectiveOp() {
        return opFilter;
    }
    
    public Op getOp() {
        return op;
    }

    @Override
    public QueryIterator eval(QueryIterator input, ExecutionContext execCxt) {
        QueryIterator temp = QC.execute(opFilter.getSubOp(), input, execCxt);
        return new QueryIterFilterWithHistoryExpr(temp, expr, execCxt, varName, state);
    }

    @Override
    public void outputArgs(IndentedWriter out, SerializationContext sCxt) {
//        out.println(FmtUtils.stringForNode(node, sCxt)) ;
//        out.ensureStartOfLine();
//        WriterOp.output(out, subOp, sCxt);
        WriterOp.output(out, opFilter, sCxt);
    }

    @Override
    public int hashCode() {
        return opFilter.hashCode() ^ 1337;
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpFilterWithHistory)) {
            return false;
        }
        OpFilterWithHistory otherOp = (OpFilterWithHistory) other;
        return opFilter.equalTo(otherOp.opFilter, labelMap);
    }

}
