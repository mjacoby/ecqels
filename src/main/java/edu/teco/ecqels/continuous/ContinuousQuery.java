/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.hp.hpl.jena.query.Query;
import edu.teco.ecqels.event.NewQueryResultAvailableListener;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public interface ContinuousQuery<T extends ContinuousListener> extends NewQueryResultAvailableListener {

    public Query getQuery();

    public void addListener(T listener);

    public void removeListener(T listener);
}
