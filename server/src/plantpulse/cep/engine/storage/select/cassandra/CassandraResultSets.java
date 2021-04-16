package plantpulse.cep.engine.storage.select.cassandra;

import java.util.List;
import java.util.concurrent.Future;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * <B>CassandraResultSets</B>
 * 
 * https://www.datastax.com/dev/blog/java-driver-async-queries
 * 
 * Utility methods to demonstrate how to compose result set futures.
 * 
 */
public class CassandraResultSets {

    /**
     * Executes the same query on different partitions, and returns all the results as a list.
     *
     * @param session the {@code Session} to query Cassandra.
     * @param query a query string with a single bind parameter for the partition key.
     * @param partitionKeys the list of partition keys to execute the query on.
     *
     * @return a future that will complete when all the queries have completed, and contain the list of matching rows.
     */
    public static Future<List<ResultSet>> queryAllAsList(Session session, String query, Object... partitionKeys) {
        List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
        return Futures.successfulAsList(futures);
    }

    /**
     * Executes the same query on different partitions, and returns the results as they become available.
     *
     * @param session the {@code Session} to query Cassandra.
     * @param query a query string with a single bind parameter for the partition key.
     * @param partitionKeys the list of partition keys to execute the query on.
     *
     * @return a list of futures in the order of their completion.
     */
    public static List<ListenableFuture<ResultSet>> queryAll(Session session, String query, Object... partitionKeys) {
        List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
        return Futures.inCompletionOrder(futures);
    }

    /**
     * Executes the same query on different partitions, and returns an {code Observable} that emits the results as they become available.
     *
     * @param session the {@code Session} to query Cassandra.
     * @param query a query string with a single bind parameter for the partition key.
     * @param partitionKeys the list of partition keys to execute the query on.
     *
     * @return the observable.
     */
    public static Observable<ResultSet> queryAllAsObservable(Session session, String query, Object... partitionKeys) {
        List<ResultSetFuture> futures = sendQueries(session, query, partitionKeys);
        Scheduler scheduler = Schedulers.io();
        List<Observable<ResultSet>> observables = Lists.transform(futures, (ResultSetFuture future) -> Observable.from(future, scheduler));
        return Observable.merge(observables);
    }

    /**
     * sendQueries
     * 
     * @param session
     * @param query
     * @param partitionKeys
     * @return
     */
    private static List<ResultSetFuture> sendQueries(Session session, String query, Object[] partitionKeys) {
        List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(partitionKeys.length);
        for (Object partitionKey : partitionKeys)
            futures.add(session.executeAsync(query, partitionKey));
        return futures;
    }
}
