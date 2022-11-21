/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

/**
 * Multi-threaded MVCC (multi version concurrency) test cases.
 */
public class TestMvccMissingRows extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        for (int i = 0; i < 10000; ++i) {
            TestBase.createCaller().init().testFromMain();
        }
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void test() throws Exception {
        testInsertingAndDeletingWithForeignKey();
    }

    private static final Logger LOG = Logger.getLogger("TestMvccMissingRows");

    private void testInsertingAndDeletingWithForeignKey() throws Exception {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table parent(parent_id varchar not null primary key, parent_value varchar)");
        stat.execute("create table association(child_id varchar not null primary key, parent_id varchar not null, FOREIGN KEY (parent_id) REFERENCES parent(parent_id), FOREIGN KEY (parent_id) REFERENCES parent(parent_id))");

        String parentUUID = UUID.randomUUID().toString();
        PreparedStatement insertParent = conn.prepareStatement("insert into parent(parent_id, parent_value) values(?, ?)");
        insertParent.setString(1, parentUUID);
        insertParent.setString(2, "parent");
        if (insertParent.executeUpdate() != 1) {
            throw new AssertionError("didn't insert one row");
        }

        int count = 200;
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CompletableFuture allFutures = CompletableFuture.completedFuture(null);
        ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String index = Integer.toString(i);
            CompletableFuture future = CompletableFuture.runAsync(new Runnable() {
                @Override
                public void run() {
                    try {
                        String childUUID = UUID.randomUUID().toString();
                        {
                            LOG.info("Starting creating tx");
                            Connection conn = getConnectionFromPool();
                            conn.setAutoCommit(false);

                            PreparedStatement insertParent = conn.prepareStatement("insert into parent(parent_id, parent_value) values(?, ?)");
                            insertParent.setString(1, childUUID);
                            insertParent.setString(2, "child" + index);
                            if (insertParent.executeUpdate() != 1) {
                                throw new AssertionError("didn't insert one row");
                            }

                            PreparedStatement insertChild = conn.prepareStatement("insert into association(child_id, parent_id) values(?, ?)");
                            insertChild.setString(1, childUUID);
                            insertChild.setString(2, parentUUID);
                            if (insertChild.executeUpdate() != 1) {
                                throw new AssertionError("didn't insert one row");
                            }

                            conn.commit();
                            LOG.info("Ending creating tx");
                            returnConnectionToPool(conn);
                        }

                        {
                            LOG.info("Starting reading tx");
                            Connection conn = getConnectionFromPool();
                            conn.setAutoCommit(false);
                            PreparedStatement selectParentAndChildData = conn.prepareStatement("SELECT parent.parent_id, child.parent_id\n" +
                                    "    FROM parent as parent\n" +
                                    "    JOIN association\n" +
                                    "        ON association.parent_id = parent.parent_id\n" +
                                    "    JOIN parent as child\n" +
                                    "        ON (association.child_id = child.parent_id) WHERE parent.parent_id = ?;");

                            selectParentAndChildData.setString(1, parentUUID);

                            ResultSet resultSet = selectParentAndChildData.executeQuery();
                            List<String> childEntities = new ArrayList<>();
                            while (resultSet.next()) {
                                childEntities.add(resultSet.getString(2));
                            }
                            LOG.info("Returned child entities: " + childEntities.size());
                            assertTrue(childEntities.contains(childUUID));
                            conn.commit();
                            LOG.info("Ending reading tx");
                            returnConnectionToPool(conn);
                        }

                        {
                            LOG.info("Starting deleting tx");
                            Connection conn = getConnectionFromPool();
                            conn.setAutoCommit(false);

                            // Find all parents that contains child we are trying to remove
                            PreparedStatement selectAllParentsForChild = conn.prepareStatement("SELECT parent.parent_id, child.parent_id\n" +
                                    "    FROM parent as parent\n" +
                                    "    JOIN association\n" +
                                    "        ON association.parent_id = parent.parent_id\n" +
                                    "    JOIN parent as child\n" +
                                    "        ON (association.child_id = child.parent_id) WHERE child.parent_id = ?;");

                            selectAllParentsForChild.setString(1, childUUID);
                            ResultSet resultSet = selectAllParentsForChild.executeQuery();
                            // Should be always parentUUID
                            if (resultSet.next()) {
                                assertEquals(parentUUID, resultSet.getString(1));
                            } else {
                                throw new AssertionError("didn't find the child row when joining with the parent");
                            }

                            // Load parent from the database with all children (done by hibernate so we are able to call removeAssociatedEntity on it)
                            PreparedStatement selectParentAndChildData = conn.prepareStatement("SELECT parent.parent_id, child.parent_id\n" +
                                    "    FROM parent as parent\n" +
                                    "    JOIN association\n" +
                                    "        ON association.parent_id = parent.parent_id\n" +
                                    "    JOIN parent as child\n" +
                                    "        ON (association.child_id = child.parent_id) WHERE parent.parent_id = ?;");
                            selectParentAndChildData.setString(1, parentUUID);

                            ResultSet selectParentAndChildDataResultSet = selectParentAndChildData.executeQuery();
                            List<String> childEntities = new ArrayList<>();
                            while (selectParentAndChildDataResultSet.next()) {
                                childEntities.add(resultSet.getString(2));
                            }
                            LOG.info("Returned child entities: " + childEntities.size());
                            assertTrue(childEntities.contains(childUUID)); // This is where Keycloak code return wrong result

                            PreparedStatement deleteAssociation = conn.prepareStatement("delete from association where child_id = ? and parent_id = ?");
                            deleteAssociation.setString(1, childUUID);
                            deleteAssociation.setString(2, parentUUID);
                            if (deleteAssociation.executeUpdate() != 1) {
                                throw new AssertionError("didn't delete one row");
                            }

                            PreparedStatement deleteChild = conn.prepareStatement("delete from parent where parent_id = ?");
                            deleteChild.setString(1, childUUID);
                            if (deleteChild.executeUpdate() != 1) {
                                throw new AssertionError("didn't delete one row");
                            }
                            conn.commit();
                            LOG.info("Ending deleting tx");
                            returnConnectionToPool(conn);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, executor).whenComplete(new BiConsumer<Void, Throwable>() {
                @Override
                public void accept(Void unused, Throwable throwable) {
                    if (throwable != null) {
                        throwable.printStackTrace();
                    }
                }
            });

            allFutures = CompletableFuture.allOf(allFutures, future);
        }

        allFutures.get();
        executor.shutdown();
        closeConnectionPool();
        conn.close();
        deleteDb(getTestName());
    }

    HashSet<Connection> connectionPool = new HashSet<>();

    private Connection getConnectionFromPool() throws SQLException {
        Connection result = null;
        synchronized (this) {
            if (connectionPool.size() > 0) {
                result = connectionPool.stream().iterator().next();
                connectionPool.remove(result);
            }
        }
        if (result == null) {
            result = getConnection(getTestName());
        }
        return result;
    }

    private void returnConnectionToPool(Connection connection) throws SQLException {
        synchronized (this) {
            connection.setAutoCommit(true);
            connectionPool.add(connection);
        }
    }

    private void closeConnectionPool() throws SQLException {
        for (Connection connection : connectionPool) {
            connection.close();
        }
    }

}