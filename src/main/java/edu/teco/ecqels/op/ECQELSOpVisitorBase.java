/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.op;

import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import edu.teco.ecqels.lang.op.OpRefreshableGraph;
import edu.teco.ecqels.lang.op.OpRefreshableGraph;
import edu.teco.ecqels.lang.op.OpRefreshableService;
import edu.teco.ecqels.lang.op.OpRefreshableService;
import edu.teco.ecqels.lang.op.OpRefreshableStream;
import edu.teco.ecqels.lang.op.OpRefreshableStream;
import edu.teco.ecqels.lang.op.OpStream;
import edu.teco.ecqels.lang.op.OpStream;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class ECQELSOpVisitorBase extends OpVisitorBase {

    @Override
    public void visit(OpExt opExt) {
        if (opExt instanceof OpStream) {
            visit((OpStream) opExt);
        } else if (opExt instanceof OpRefreshableStream) {
            visit((OpRefreshableStream) opExt);
        } else if (opExt instanceof OpRefreshableService) {
            visit((OpRefreshableService) opExt);
        } else if (opExt instanceof OpRefreshableGraph) {
            visit((OpRefreshableGraph) opExt);
        } else{
            super.visit(opExt);
        }
    }

    public void visit(OpStream opStream) {

    }

    public void visit(OpRefreshableGraph opGraph) {

    }

    public void visit(OpRefreshableService opService) {

    }

    public void visit(OpRefreshableStream opStream) {

    }
}
