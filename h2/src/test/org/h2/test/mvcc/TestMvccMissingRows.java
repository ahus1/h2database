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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void test() throws Exception {
        testInsertingAndDeletingWithForeignKey();
    }

    private void testInsertingAndDeletingWithForeignKey() throws Exception {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table parent(parent_id varchar not null primary key, parent_value varchar)");
        stat.execute("create table child(child_id varchar not null primary key, parent_id varchar not null, child_value varchar, FOREIGN KEY (parent_id) REFERENCES parent(parent_id))");
        int count = 300;
        ExecutorService executor = Executors.newFixedThreadPool(5);
        ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Task task = new Task() {
                @Override
                public void call() throws Exception {
                    String parentUuid = UUID.randomUUID().toString();
                    {
                        Connection conn = getConnectionFromPool();
                        conn.setAutoCommit(false);
                        String childUuid = UUID.randomUUID().toString();
                        PreparedStatement insertParent = conn.prepareStatement("insert into parent(parent_id, parent_value) values(?, ?)");
                        insertParent.setString(1, parentUuid);
                        insertParent.setString(2, "parent");
                        if (insertParent.executeUpdate() != 1) {
                            throw new AssertionError("didn't insert one row");
                        }
                        PreparedStatement insertChild = conn.prepareStatement("insert into child(child_id, parent_id, child_value) values(?, ?, ?)");
                        insertChild.setString(1, childUuid);
                        insertChild.setString(2, parentUuid);
                        insertChild.setString(3, "child");
                        if (insertChild.executeUpdate() != 1) {
                            throw new AssertionError("didn't insert one row");
                        }
                        conn.commit();
                        returnConnectionToPool(conn);
                    }

                    {
                        Connection conn = getConnectionFromPool();
                        conn.setAutoCommit(false);
                        PreparedStatement selectParentAndChildData = conn.prepareStatement("SELECT parent.parent_id, child.child_id\n" +
                                "    FROM parent\n" +
                                "    JOIN child\n" +
                                "        ON (child.parent_id = parent.parent_id) WHERE parent.parent_id = ?;");
                        selectParentAndChildData.setString(1, parentUuid);
                        ResultSet resultSet = selectParentAndChildData.executeQuery();
                        String childUuid;
                        if (resultSet.next()) {
                            childUuid = resultSet.getString(2);
                        } else {
                            throw new AssertionError("didn't find the child row when joining with the parent");
                        }
                        PreparedStatement deleteChild = conn.prepareStatement("delete from child where child_id = ?");
                        deleteChild.setString(1, childUuid);
                        if (deleteChild.executeUpdate() != 1) {
                            throw new AssertionError("didn't delete one row");
                        }
                        PreparedStatement deleteParent = conn.prepareStatement("delete from parent where parent_id = ?");
                        deleteParent.setString(1, parentUuid);
                        if (deleteParent.executeUpdate() != 1) {
                            throw new AssertionError("didn't delete one row");
                        }
                        conn.commit();
                        returnConnectionToPool(conn);
                    }
                }
            };
            futures.add(executor.submit(task));
        }
        executor.shutdown();
        for (Future<?> future : futures) {
            future.get();
        }
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
