package org.voltdb.utils;

import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Ben.Gross on 7/11/2014.
 */
public class VoltDBReader {

    Class c;
    Connection conn;
    Client client;
    LoaderConfig config;
    Statement jdbcStmt;
    int records = 0;
    String currentQuery;
    String currentVoltProcedure;

    public VoltDBReader(LoaderConfig config) {
        this.config = config;
    }

    public void connectToSource() throws Exception {
        // load JDBC driver
        c = Class.forName(config.jdbcdriver);
        System.out.println("Connecting to source database with url: " + config.jdbcurl);
        conn = DriverManager.getConnection(config.jdbcurl, config.jdbcuser, config.jdbcpassword);
    }

    public void connectToVoltDB() throws InterruptedException {
        System.out.println("Connecting to VoltDB on: " + config.volt_servers);

        ClientConfig cc = new ClientConfig(config.volt_user, config.volt_password);
        client = ClientFactory.createClient(cc);

        String servers = config.volt_servers;
        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectVoltNode(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    void connectVoltNode(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            } catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    public void loadTables(String tableNames, String procNames) throws SQLException, IOException, InterruptedException {
        String[] tableNameArray = tableNames != null && !"".equals(tableNames) ? tableNames.split(",") : null;
        String[] procNameArray = procNames != null && !"".equals(procNames) ? procNames.split(",") : null;

        for (int j = 0; j < tableNameArray.length && tableNameArray != null; j++) {
            String tableName = tableNameArray[j];
            String procName = procNameArray != null ? procNameArray[j] : "";

            // if procName not provided, use the default VoltDB TABLENAME.insert procedure
            if (procName.length() == 0) {
                if (tableName.contains("..")) {
                    procName = tableName.split("\\.\\.")[1].toUpperCase() + ".insert";
                } else {
                    procName = tableName.toUpperCase() + ".insert";
                }
            }

            // query the table
            String jdbcSelect = "SELECT * FROM " + tableName + " limit 10;";

            printData(jdbcSelect, procName);
        }
    }

    private void printData(String sourceSelectQuery, String voltProcedure) throws SQLException, IOException, InterruptedException {
        this.currentQuery = sourceSelectQuery;
        this.currentVoltProcedure = voltProcedure;

        System.out.println("Querying source database: " + sourceSelectQuery);
        long t1 = System.currentTimeMillis();
        ResultSet rs = jdbcStmt.executeQuery(sourceSelectQuery);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();
        System.out.println("Column count: " + columns);
        System.out.println("Column 1: " + rsmd.getColumnClassName(1) + " " + rsmd.getColumnName(1));
//        System.out.println("     " + rs.getLong(1));
        System.out.println("Column 2: " + rsmd.getColumnClassName(2) + " " + rsmd.getColumnName(2));
//        System.out.println("     " + rs.getString(1));
        System.out.println("Column 3: " + rsmd.getColumnClassName(3) + " " + rsmd.getColumnName(3));
//        System.out.println("     " + rs.getInt(1));

        long t2 = System.currentTimeMillis();
        float sec1 = (t2 - t1) / 1000.0f;
        System.out.format("Query took %.3f seconds to return first row, with fetch size of %s records.%n", sec1, config.fetchsize);

        ParallelProcessor.Monitor mr = new ParallelProcessor.Monitor<ArrayList<Object[]>>(config.maxQueueSize);
        SourceReader sr = new SourceReader(mr, rs, columns);
        sr.producerTask();

        long t3 = System.currentTimeMillis();
        float sec2 = (t3 - t2) / 1000.0f;
        float tps = records / sec2;

        System.out.format("Pulled %d requests in %.3f seconds at a rate of %f TPS.%n", records, sec2, tps);
        LoaderCallback.printProcedureResults(voltProcedure);

        records = 0;
    }

    private class SourceReader extends ParallelProcessor.Producer<ArrayList<Object[]>> {
        ResultSet rs;
        int columns;

        SourceReader(ParallelProcessor.Monitor<ArrayList<Object[]>> monitor, ResultSet rs, int columns) {
            super(monitor);
            this.rs = rs;
            this.columns = columns;
        }

        protected void producerTask() {
            ArrayList<Object[]> arrayList = new ArrayList<Object[]>();
            try {
                while (rs.next()) {
                    records++;
                    // get one record of data as an Object[]
                    Object[] columnValues = new Object[columns];
                    for (int i = 0; i < columns; i++) {
                       columnValues[i] = rs.getObject(i + 1);
                        System.out.println(columnValues[i]);
                    }

                    arrayList.add(columnValues);

                    if (records % config.fetchsize == 0) {
                        System.out.println("Pulled " + config.fetchsize + " records from the source");

                        monitor.jobQueue.put(arrayList);

                        arrayList = new ArrayList<Object[]>();
                    }
                }

                System.out.println("Pulled " + records % config.fetchsize + " records from the source");

                monitor.jobQueue.put(arrayList);

//                controller.signalProducer(false);
            } catch (SQLException e) {
                System.out.println(Thread.currentThread().getName() + " - Exception occurred while executing the query: " + currentQuery);
                e.printStackTrace();
//                controller.signalProducer(false);
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " - Exception occurred while executing the query: " + currentQuery);
                e.printStackTrace();
//                controller.signalProducer(false);
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " - unexpected exception occurred while executing the query: " + currentQuery);
                e.printStackTrace();
//                controller.signal(false);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        LoaderConfig cArgs = LoaderConfig.getConfig("VoltDBLoader", args);

        VoltDBReader reader = new VoltDBReader(cArgs);
        reader.connectToSource();
        reader.connectToVoltDB();

        reader.jdbcStmt = reader.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        reader.jdbcStmt.setFetchSize(reader.config.fetchsize);

        reader.loadTables(cArgs.tablename, cArgs.procname);
    }
}
