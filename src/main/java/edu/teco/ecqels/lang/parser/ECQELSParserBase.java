package edu.teco.ecqels.lang.parser;


import com.hp.hpl.jena.sparql.lang.ParserQueryBase;
import edu.teco.ecqels.lang.window.Duration;


public class ECQELSParserBase extends ParserQueryBase
    implements ECQELSParserConstants
{
	public Duration getDuration(String str){
		return new Duration(str);
	}
}
