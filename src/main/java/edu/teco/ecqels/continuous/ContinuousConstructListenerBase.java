/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.continuous;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingUtils;
import com.hp.hpl.jena.sparql.modify.TemplateLib;
import com.hp.hpl.jena.sparql.syntax.Template;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class ContinuousConstructListenerBase implements ContinuousConstructListener {

    protected Template template;


    @Override
    public void update(List<List<Triple>> triples) {

    }

    @Override
    public void setTemplate(Template template) {
        this.template = template;
    }

    @Override
    public void update(QueryIterator result) {
        List<List<Triple>> temp = new ArrayList<>();
        while(result.hasNext()) {
            Binding binding = result.next();
            List<Triple> triples = new ArrayList<>();
            for (Triple triple : template.getTriples()) {
                BindingUtils r = new BindingUtils();
                triples.add(TemplateLib.subst(triple, binding, null));
            }
            temp.add(triples);
        }
        update(temp);
    }

}
