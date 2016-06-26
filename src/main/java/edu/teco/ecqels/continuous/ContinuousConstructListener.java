/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.syntax.Template;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public interface ContinuousConstructListener extends ContinuousListener {
    public void update(List<List<Triple>> triples);
    
    public void setTemplate(Template t);
}
