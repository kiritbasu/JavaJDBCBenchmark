package org.kirit.jdbcbenchmark;


import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.*;

public class JavaJDBCBenchmark {
    private static final String dbClassName = "com.mysql.jdbc.Driver";
    private static final String CONNECTION = "jdbc:mysql://localhost:3306/";
    private static final String USER = "kirit";
    private static final String PASSWORD = "kirit";
    private static void executeSQL(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
    private static void ResetEnvironment() throws SQLException {
        Properties p = new Properties();
        p.put("user", USER);
        p.put("password", PASSWORD);
        try (Connection conn = DriverManager.getConnection(CONNECTION, p)) {
            for (String query: new String[] {
                    "DROP DATABASE IF EXISTS test",
                    "CREATE DATABASE test",
                    "USE test",
                    "CREATE TABLE tbl (id INT AUTO_INCREMENT PRIMARY KEY, `val` int(11) DEFAULT NULL)"
            }) {
                executeSQL(conn, query);
            }
        }
    }
    private static void worker(int nCount, int nBatchSize, boolean bRewriteBatchSt) {
        Properties properties = new Properties();
        properties.put("user", USER);
        properties.put("password", PASSWORD);
        final String ConnString =  bRewriteBatchSt ? CONNECTION + "?rewriteBatchedStatements=true": CONNECTION;
        try (Connection conn = DriverManager.getConnection(ConnString, properties)) {
            executeSQL(conn, "USE test");
            //warmup
            executeSQL(conn, "INSERT INTO tbl (id, val) VALUES (null, 1)");

            String query = "INSERT INTO tbl (val) VALUES (?)";
            PreparedStatement statement = conn.prepareStatement(query);

            System.out.println("Inserting " + nCount + " rows into table.");
            long startTime = System.nanoTime();

            for (int i = 1; i <= nCount; i++) {

                statement.setInt(1, i);

                statement.addBatch();
                if(i % nBatchSize == 0) {
                    statement.executeBatch();
                }

            }
            //execute final batch
            statement.executeBatch();

            long endTime = System.nanoTime();
            double duration = (double)(endTime - startTime) / (Math.pow(10, 9));
            System.out.println("Thread " + Thread.currentThread().getId() + " : "
                    + " Inserting " + nCount + " rows w/ batch size " + nBatchSize
                    + " : Result - " + Math.round(nCount/duration) + " inserts per second" );

            statement.close();
            conn.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {

        int _thread_count = 1;
        int _nRows = 0;
        int _nBatchSize = 0;
        boolean _bRewriteBatchSt = false;

        CommandLineParser parser = new DefaultParser();

        Options options = new Options();

        options.addOption(Option.builder("b")
                .longOpt("batchsize")
                .required(true)
                .hasArg()
                .desc("specify batch size")
                .build());
        options.addOption(Option.builder("n")
                .longOpt("numberofrows")
                .required(true)
                .hasArg()
                .desc("specify # of rows to insert")
                .build());
        options.addOption(Option.builder("t")
                .longOpt("numberofthreads")
                .required(true)
                .hasArg()
                .desc("specify number of parallel threads to execute")
                .build());
        options.addOption( Option.builder("r")
                .desc("Use rewriteBatchedStatements in connection string")
                .hasArg()
                .build());
        options.addOption( "f", "full", false, "Use multi-column insert statement (all datatypes)" );
        options.addOption(Option.builder("help").build());


        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if( line.hasOption( "b" ) ) {
                _nBatchSize = Integer.parseInt(line.getOptionValue("b"));
            }

            if( line.hasOption( "n" ) ) {
                _nRows = Integer.parseInt(line.getOptionValue("n"));
            }

            if( line.hasOption( "t" ) ) {
                _thread_count = Integer.parseInt(line.getOptionValue("t"));
            }

            if( line.hasOption( "r" ) ) {
                _bRewriteBatchSt = Boolean.parseBoolean(line.getOptionValue( "r" ));
            }


            if (line.hasOption("help")) {
                String header = "Run benchmarks using Java JDBC\n\n";
                String footer = "Ask Kirit for help";

                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("JavaJDBCBenchmark", header, options, footer, true);
            }
        }
        catch( ParseException exp ) {
            System.out.println( "Error : " + exp.getMessage() );
            System.exit(1);
        }

        Class.forName(dbClassName);
        ResetEnvironment();
        System.out.println("Executing " + _thread_count + " threads");
        ExecutorService executor = Executors.newFixedThreadPool(_thread_count);
        for (int i = 0; i < _thread_count; i++) {
            final int nCount = _nRows;
            final int nBatchSize = _nBatchSize;
            final boolean bRewriteBatchSt = _bRewriteBatchSt;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    worker(nCount, nBatchSize, bRewriteBatchSt);
                }
            });
        }
        Thread.sleep(20000);
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            System.err.println("Pool did not terminate");
        }
    }
}
