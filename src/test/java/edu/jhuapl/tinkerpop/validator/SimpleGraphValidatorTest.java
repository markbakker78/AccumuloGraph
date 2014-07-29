package edu.jhuapl.tinkerpop.validator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;

import edu.jhuapl.tinkerpop.AccumuloGraphTestUtils;

public class SimpleGraphValidatorTest {

	@Test
	public void testValidGraph() {
		Graph g = GraphFactory.open(AccumuloGraphTestUtils.generateGraphConfig(
				getClass().getSimpleName() + "_1").getConfiguration());
		for (int i = 0; i < 100; i++) {
			g.addVertex("" + i);
		}
		Random r = new Random();
		for (int i = 0; i < 100; i++) {
			g.addEdge(null, g.getVertex("" + r.nextInt(100)),
					g.getVertex("" + r.nextInt(100)), "k");
		}
		GraphValidator gv = new SimpleGraphValidator();
		assertTrue(gv.validate(g));
	}

	@Test
	public void testValidGraphWithoutChecks() {
		Graph g = GraphFactory.open(AccumuloGraphTestUtils
				.generateGraphConfig(getClass().getSimpleName() + "_2")
				.skipExistenceChecks(true).getConfiguration());
		for (int i = 0; i < 100; i++) {
			g.addVertex("" + i);
		}
		Random r = new Random();
		for (int i = 0; i < 100; i++) {
			g.addEdge(null, g.getVertex("" + r.nextInt(100)),
					g.getVertex("" + r.nextInt(100)), "k");
		}
		GraphValidator gv = new SimpleGraphValidator();
		assertTrue(gv.validate(g));
	}

	/*
	 * Could not think of how to make an invalid graph. 
	 * 
	 * @Test
	 
	public void testInvalidGraph() {
		Graph g = GraphFactory.open(AccumuloGraphTestUtils
				.generateGraphConfig(getClass().getSimpleName() + "_3")
				.skipExistenceChecks(true).getConfiguration());
		for (int i = 0; i < 100; i++) {
			g.addVertex("" + i);
		}
		Random r = new Random();
		for (int i = 0; i < 100; i++) {
			g.addEdge(null, g.getVertex("" + r.nextInt(100)),
					g.getVertex("" + r.nextInt(100)), "k");
		}
		Edge e = g.addEdge(null, g.getVertex("1"), g.getVertex("2"), "k");
		System.out.println(g.getEdge(e.getId()));
		g.removeVertex(g.getVertex("1"));

		g.shutdown();
		GraphValidator gv = new SimpleGraphValidator();
		g = GraphFactory.open(AccumuloGraphTestUtils
				.generateGraphConfig(getClass().getSimpleName() + "_3")
				.skipExistenceChecks(false).getConfiguration());
		System.out.println(g.getEdge(e.getId()));
		assertFalse(gv.validate(g));
	}*/
}
