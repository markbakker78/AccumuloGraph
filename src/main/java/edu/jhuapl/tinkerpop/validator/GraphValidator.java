package edu.jhuapl.tinkerpop.validator;

import com.tinkerpop.blueprints.Graph;

public interface GraphValidator {
	public boolean validate(Graph g);
}
