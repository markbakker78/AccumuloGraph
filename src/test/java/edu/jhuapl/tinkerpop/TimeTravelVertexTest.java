package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;

public class TimeTravelVertexTest {

    @Test
    public void SimpleTestVertexTimeTravel(){
        String[] preloadedProperties = {"name"};
        AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration().setInstanceType(AccumuloGraphConfiguration.InstanceType.Mini)
                .setCreate(true)
                .setUser("root")
                .setPassword("")
                .setAutoFlush(true)
                .setLruMaxCapacity(0) //turn LRU cache off
                //.setPreloadedProperties(preloadedProperties)
                .setGraphName("myGraph");
        cfg.setMiniClusterTempDir("/Users/mark/accumulomini");
        AccumuloGraph graph = (AccumuloGraph)GraphFactory.open(cfg.getConfiguration());

        long t1 = System.currentTimeMillis();
        AccumuloVertex v1 = (AccumuloVertex)graph.addVertex("1",t1);
        v1.setProperty("name", "Mark", t1);
        graph.flush();
        long t2 = t1+5;
        long t3 = t1+10;
        AccumuloVertex v2 = (AccumuloVertex)graph.addVertex("1",t3);
        v2.setProperty("name", "Lodewijk", t3);
        graph.flush();
        long t4 = t2+10;

        graph.flush();

        graph.setTimestampFilter(null, t4);

        Vertex rv1 = graph.getVertex("1");
        graph.preloadProperties((AccumuloElement)rv1,Vertex.class);

        System.out.println(rv1.getProperty("name"));

        //Edge e1 = graph.addEdge("E1", v1, v2, "knows");
        //e1.setProperty("since", new Date());

    }
}
