package edu.jhuapl.tinkerpop.validator;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;

public class SimpleGraphValidator implements GraphValidator {

	@Override
	public boolean validate(Graph g) {

		for (Edge e : g.getEdges()) {

			if (g.getVertex(e.getVertex(Direction.IN).getId()) == null
					|| g.getVertex(e.getVertex(Direction.OUT).getId()) == null) {
				return false;
			}

		}
		return true;
	}

}
