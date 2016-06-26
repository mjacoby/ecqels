/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.sparql.core.ResultBinding;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class ContinuousSelectListenerBase implements ContinuousSelectListener {

    @Override
    public void update(List<QuerySolution> result) {
        
    }

    @Override
    public void update(QueryIterator result) {
        List<QuerySolution> temp = new ArrayList<>();
        while(result.hasNext()) {
            temp.add(new ResultBinding(null, result.next()));
        }
        result.close();
        update(temp);
    }



}
