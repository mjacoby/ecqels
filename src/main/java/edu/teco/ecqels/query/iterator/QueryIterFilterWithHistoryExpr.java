/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.query.iterator;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterFilterExpr;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprException;
import edu.teco.ecqels.op.FilterWithHistoryState;
import edu.teco.ecqels.query.execution.QueryExecutor;
import java.util.UUID;
import org.openjena.atlas.logging.Log;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class QueryIterFilterWithHistoryExpr extends QueryIterFilterExpr {

    private final Expr expr;
    private FilterWithHistoryState state;
    private final Var var;

    public QueryIterFilterWithHistoryExpr(QueryIterator input, Expr expr, ExecutionContext context, String varName, FilterWithHistoryState state) {
        super(input, expr, context);
        this.expr = expr;
        this.state = state;
        this.var = Var.alloc(varName);
    }

    @Override
    public Binding accept(Binding binding) {
        try {
            BindingMap result = BindingFactory.create(binding);
            if (expr.isSatisfied(binding, super.getExecContext())) {                                
                result.add(var, ResourceFactory.createTypedLiteral(state.getHistory()).asNode());
                state.setHistory(true);
                return result;
            } else {
                result.add(QueryExecutor.ABORT_QUERY_VARIABLE, ResourceFactory.createTypedLiteral(true).asNode());
                state.setHistory(false);
            }            
            return null;
        } catch (ExprException ex) { // Some evaluation exception
            return null;
        } catch (Exception ex) {
            Log.warn(this, "General exception in " + expr, ex);
            return null;
        }
    }
}
