/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class ContinuousConstruct extends AbstractContinuousQuery<ContinuousConstructListener>{

    public ContinuousConstruct(Query query, ExecutionContext executionContext) {
        super(query, executionContext);
    }
    
}
