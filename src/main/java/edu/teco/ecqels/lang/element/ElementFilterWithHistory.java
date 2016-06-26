/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.lang.element;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import org.openjena.atlas.io.IndentedWriter;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class ElementFilterWithHistory extends ElementFilter {
    
    private final String varName;
    
    public ElementFilterWithHistory(Expr expr, String varName) {
        super(expr);
        this.varName = varName;
    }       
    
    public String getVarName() {
        return varName;                
    }
    
}
