/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.hp.hpl.jena.query.QuerySolution;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public interface ContinuousSelectListener extends ContinuousListener {
    
    public void update(List<QuerySolution> result);
}
