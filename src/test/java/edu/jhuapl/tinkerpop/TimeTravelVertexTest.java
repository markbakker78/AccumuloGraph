package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedSet;

import static edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType.Mini;
import static edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType.Mock;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test AccumuloGraph with timestamp versioning added. Using the timestampFilter you can choose a point in time to
 * retrieve vertex, edges and properties.
 */
public class TimeTravelVertexTest {

  private static final Logger LOG = LoggerFactory.getLogger(TimeTravelVertexTest.class);
  private final AccumuloGraph graph;

  /**
   * Create a fresh AccumuloGraph instance in Mini mode, saving actual files on disk /w Zookeeper.
   */
  public TimeTravelVertexTest() {
    AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration().setInstanceType(Mini)
        .setCreate(true)
        .setUser("root")
        .setPassword("")
        .setAutoFlush(true)
        .setLruMaxCapacity(0) //turn LRU cache off
        .setGraphName("myGraph");

    cfg.skipExistenceChecks(false);

    graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
  }

  @Before
  public void recreateTablesAndDisableTimestampFilter() throws Exception {
    graph.clear();
    graph.disableTimestampFilterOnThisThread();

    // Default the versioning is disabled for all tables. (set to max 1 version)
    setScanMaxVersionsOnTables(graph, 2);
  }

  @Test
  public void simpleVertexTimetravelTest() throws Exception {
    long time1 = System.currentTimeMillis();
    long time0 = time1 - 5;
    long time2 = time1 + 5;

    AccumuloVertex newVertex = (AccumuloVertex) graph.addVertex("vertex1", time1);
    newVertex.setProperty("name", "Name@Time1", time1);
    newVertex.setProperty("name", "Name@Time2", time2); // overwrite 5ms later.
    // graph.flush() not required: autoFlush enabled.


    Vertex v1;
    String pName;

    // Should not find vertex1 @ time0, it was inserted at time1
    graph.enableTimestampFilterOnThisThread(null, time0);
    v1 = graph.getVertex("vertex1");
    assertThat(v1, nullValue());

    // At time1 vertex1 must be found (inclusive).
    graph.enableTimestampFilterOnThisThread(null, time1);
    v1 = graph.getVertex("vertex1");
    assertThat(v1, notNullValue());

    // Property @ time1 should be correct
    pName = v1.getProperty("name");
    assertThat(pName, is("Name@Time1"));

    graph.enableTimestampFilterOnThisThread(null, time2);
    // Vertex is still found
    v1 = graph.getVertex("vertex1");
    assertThat(v1, notNullValue());
    // And now newer version is retrieved.
    pName = v1.getProperty("name");
    assertThat(pName, is("Name@Time2"));
  }

  @Test
  public void storeVertexWithoutSpecifingTime() throws Exception {
    long time1 = System.currentTimeMillis();
    Thread.sleep(5);
    graph.addVertex("vertex1");

    // No timestamfilter, found
    assertThat(graph.getVertex("vertex1"), notNullValue());

    // Time1 before insert, NOT found
    graph.enableTimestampFilterOnThisThread(null, time1);
    assertThat(graph.getVertex("vertex1"), nullValue());

    // After Time1 vetext should be found
    graph.enableTimestampFilterOnThisThread(time1, null);
    assertThat(graph.getVertex("vertex1"), notNullValue());
  }

  /*
  From the docs: https://accumulo.apache.org/1.6/accumulo_user_manual.html#_deletes.

  Deletes are special keys in Accumulo that get sorted along will all the other data.
  When a delete key is inserted, Accumulo will not show anything that has a timestamp
  less than or equal to the delete key. During major compaction, any keys older
  than a delete key are omitted from the new file created, and the omitted keys are
  removed from disk as part of the regular garbage collection process.
  */
  @Test
  public void retrieveVertexBeforeDelete() throws Exception {
    long tsBeforeDelete = System.currentTimeMillis();
    Vertex newVertex = graph.addVertex("vertex1", tsBeforeDelete);

    Thread.sleep(5);
    
    long tsOfDeletion = System.currentTimeMillis();
    graph.removeVertex(newVertex, tsOfDeletion);

    // No timestamfilter, NOT found at this time.
    assertThat(graph.getVertex("vertex1"), nullValue());

    // Accumulo will not return anything when key is deleted..., also before deletion.
    graph.enableTimestampFilterOnThisThread(null, tsBeforeDelete);
    assertThat(graph.getVertex("vertex1"), nullValue());
  }

  @Test
  public void illegalTimestamFilter() throws Exception {
    try {
      graph.enableTimestampFilterOnThisThread(null, null);
      fail("Should use disableTimestampFilter, not initialize with null, null");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("start OR end should be specified."));
    }

    try {
      graph.enableTimestampFilterOnThisThread(2L, 1L);
      fail("Should not be able to start after end");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("Start should be earlear/equal to end."));
    }
  }

  private void setScanMaxVersionsOnTables(AccumuloGraph graph, int maxVersions) throws Exception {
    TableOperations tableOperations = graph.config.getConnector().tableOperations();
    SortedSet<String> tables = tableOperations.list();
    for (String table : tables) {
      if (table.toLowerCase().contains("meta")) {
        LOG.debug("Skipping maxVersions for table {} (containing meta)", table);
        continue;
      }

            /*
            VersioningIterator can be applied to 3 iteratorTypes:
                - scan: applied at scan time
                - minc: applied at minor compaction
                - majc: applied at major compaction
            */

      tableOperations.setProperty(table, "table.iterator.scan.vers.opt.maxVersions", String.valueOf(maxVersions));

      LOG.debug("Set maxVersions for table {} to {}.", table, maxVersions);
    }
  }
}
