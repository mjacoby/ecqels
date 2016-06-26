package edu.teco.ecqels.algebra;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.syntax.Element;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class Algebra extends com.hp.hpl.jena.sparql.algebra.Algebra {

    /**
     * Compile a query - pattern and modifiers.
     */
    public static Op compile(Query query) {
        if (query == null) {
            return null;
        }
        return new AlgebraGenerator().compile(query);
    }

    /**
     * Compile a pattern.
     */
    public static Op compile(Element elt) {
        if (elt == null) {
            return null;
        }
        return new AlgebraGenerator().compile(elt);
    }
}
