package com.metro;

import java.sql.*;
import java.util.*;
import java.math.BigDecimal;

public class MetroDW {

    public static void main(String[] args) {
        // Declaring database credentials.
        String dbUrl = "jdbc:mysql://localhost:3306/MetroDW";
        String dbUser = "root";
        String dbPassword = "W3701@jqir#";

        try {
            // Loading the MySQL JDBC Driver to enable database connections.
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Establishing a connection to the database.
            Connection dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            System.out.println("[INFO] Connected to the database!");

            // Dropping and recreating the Sales table to ensure a fresh start.
            resetSalesTable(dbConnection);

            // Starting the MESHJOIN algorithm to process data.
            System.out.println("[INFO] Initializing MESHJOIN algorithm...");
            meshJoin(dbConnection);

            // Closing the database connection after processing is complete.
            dbConnection.close();
            System.out.println("[INFO] Connection closed.");
        } catch (Exception ex) {
            // Catching and printing any errors that occur during execution.
            ex.printStackTrace();
        }
    }

    private static void resetSalesTable(Connection dbConnection) {
        try {
            // Creating a statement to execute SQL queries.
            Statement sqlStatement = dbConnection.createStatement();
            System.out.println("[INFO] Dropping and recreating the Sales table...");

            // Dropping the Sales table if it already exists in the database.
            sqlStatement.execute("DROP TABLE IF EXISTS Sales");

            // Creating a new Sales table with specified columns and data types.
            String createTableSQL = "CREATE TABLE Sales (" +
                    "Order_ID INT PRIMARY KEY, " +
                    "Order_Date DATETIME NOT NULL, " +
                    "CUSTOMER_ID INT, " +
                    "CUSTOMER_NAME VARCHAR(255), " +
                    "GENDER VARCHAR(10), " +
                    "PRODUCT_ID INT, " +
                    "PRODUCT_NAME VARCHAR(255), " +
                    "SUPPLIER_NAME VARCHAR(255), " +
                    "STORE_ID INT, " +
                    "STORE_NAME VARCHAR(255), " +
                    "QUANTITY INT NOT NULL, " +
                    "TOTAL_SALE DECIMAL(10, 2) NOT NULL" +
                    ")";
            sqlStatement.execute(createTableSQL);
            System.out.println("[INFO] Sales table recreated successfully.");
        } catch (SQLException ex) {
            // Printing an error message if resetting the table fails.
            System.err.println("[ERROR] Failed to reset Sales table.");
            ex.printStackTrace();
        }
    }

    private static void meshJoin(Connection dbConnection) {
        try {
            System.out.println("[INFO] Preparing to load transactions and master data...");

            // Fetching transaction data from the staging_Transactions table.
            System.out.println("[INFO] Loading transactions from staging_Transactions...");
            PreparedStatement fetchTransactions = dbConnection.prepareStatement(
                "SELECT Order_ID, Order_Date, PRODUCT_ID, QUANTITY, CUSTOMER_ID FROM staging_Transactions"
            );
            ResultSet transactionResults = fetchTransactions.executeQuery();

            // Initializing a queue to temporarily store transactions for processing.
            Queue<Map<String, Object>> transactionQueue = new LinkedList<>();
            // Initializing a hash table to store transactions for fast lookups.
            Map<Integer, Map<String, Object>> transactionHashTable = new HashMap<>();
            int transactionCounter = 0;

            // Iterating through the transaction result set and storing records in memory.
            while (transactionResults.next()) {
                Map<String, Object> singleTransaction = new HashMap<>();
                // Adding transaction fields to a map.
                singleTransaction.put("Order_ID", transactionResults.getInt("Order_ID"));
                singleTransaction.put("Order_Date", transactionResults.getTimestamp("Order_Date"));
                singleTransaction.put("PRODUCT_ID", transactionResults.getInt("PRODUCT_ID"));
                singleTransaction.put("QUANTITY", transactionResults.getInt("QUANTITY"));
                singleTransaction.put("CUSTOMER_ID", transactionResults.getInt("CUSTOMER_ID"));

                // Adding the transaction to the queue and hash table.
                transactionQueue.add(singleTransaction);
                transactionHashTable.put(transactionResults.getInt("Order_ID"), singleTransaction);
                transactionCounter++;

                // Logging the loaded transaction information.
                System.out.printf("[INFO] Loaded Transaction #%d | Order ID: %d%n", transactionCounter, transactionResults.getInt("Order_ID"));
            }
            System.out.printf("[INFO] Loaded %d transactions into memory.%n", transactionCounter);

            // Fetching product data from the Products table.
            System.out.println("[INFO] Loading master data and partitioning...");
            PreparedStatement fetchProducts = dbConnection.prepareStatement(
                "SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, STORE_ID FROM Products"
            );
            ResultSet productResults = fetchProducts.executeQuery();

            // Loading product data into a list of maps for processing.
            List<Map<String, Object>> productList = loadResultSetToList(productResults, "Products");

            // Partitioning product data into smaller chunks for cyclic processing.
            List<List<Map<String, Object>>> productPartitions = createPartitions(productList, 100); // Partition size of 100
            System.out.printf("[INFO] Created %d partitions for Products.%n", productPartitions.size());

            // Initializing counters for successful joins and skipped transactions.
            int successfulJoins = 0;
            int skippedTransactions = 0;
            int partitionIndex = 0;

            // Iterating through transactions and matching them with master data partitions.
            while (!transactionQueue.isEmpty()) {
                // Fetching the current partition of product data.
                List<Map<String, Object>> currentPartition = productPartitions.get(partitionIndex);
                Iterator<Map<String, Object>> transactionIterator = transactionQueue.iterator();

                // Iterating through each transaction in the queue.
                while (transactionIterator.hasNext()) {
                    Map<String, Object> currentTransaction = transactionIterator.next();
                    int productId = (int) currentTransaction.get("PRODUCT_ID");
                    int quantity = (int) currentTransaction.get("QUANTITY");

                    // Finding a matching product in the current partition.
                    Map<String, Object> matchingProduct = findById(currentPartition, "PRODUCT_ID", productId);
                    if (matchingProduct == null) {
                        // Skipping the transaction if no matching product is found.
                        skippedTransactions++;
                        continue;
                    }

                    // Converting the product price from BigDecimal to Double for calculations.
                    double productPrice = ((BigDecimal) matchingProduct.get("PRODUCT_PRICE")).doubleValue();

                    // Calculating the total sale for the transaction.
                    double totalSale = quantity * productPrice;

                    // Inserting the enriched transaction into the Sales table.
                    PreparedStatement insertIntoSales = dbConnection.prepareStatement(
                        "INSERT INTO Sales (Order_ID, Order_Date, CUSTOMER_ID, CUSTOMER_NAME, GENDER, PRODUCT_ID, " +
                        "PRODUCT_NAME, SUPPLIER_NAME, STORE_ID, STORE_NAME, QUANTITY, TOTAL_SALE) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    );
                    insertIntoSales.setInt(1, (int) currentTransaction.get("Order_ID"));
                    insertIntoSales.setTimestamp(2, (Timestamp) currentTransaction.get("Order_Date"));
                    insertIntoSales.setInt(3, (int) currentTransaction.get("CUSTOMER_ID"));
                    insertIntoSales.setString(4, "Customer Name"); // Placeholder for customer name.
                    insertIntoSales.setString(5, "Gender"); // Placeholder for gender.
                    insertIntoSales.setInt(6, productId);
                    insertIntoSales.setString(7, (String) matchingProduct.get("PRODUCT_NAME"));
                    insertIntoSales.setString(8, "Supplier Name"); // Placeholder for supplier name.
                    insertIntoSales.setInt(9, (int) matchingProduct.get("STORE_ID"));
                    insertIntoSales.setString(10, "Store Name"); // Placeholder for store name.
                    insertIntoSales.setInt(11, quantity);
                    insertIntoSales.setDouble(12, totalSale);
                    insertIntoSales.executeUpdate();

                    // Removing the processed transaction from the queue.
                    transactionIterator.remove();
                    successfulJoins++;
                    System.out.printf("[SUCCESS] Processed Transaction | Order ID: %d | Total Sale: %.2f%n", currentTransaction.get("Order_ID"), totalSale);
                }

                // Moving to the next product partition for the next iteration.
                partitionIndex = (partitionIndex + 1) % productPartitions.size();
            }

            // Printing a summary of the MESHJOIN process.
            System.out.println("[INFO] MESHJOIN Processing Summary:");
            System.out.printf("    Total Transactions Processed: %d%n", transactionCounter);
            System.out.printf("    Successful Joins: %d%n", successfulJoins);
            System.out.printf("    Skipped Transactions: %d%n", skippedTransactions);

        } catch (SQLException ex) {
            // Catching and printing SQL-related errors.
            ex.printStackTrace();
        }
    }

    private static List<Map<String, Object>> loadResultSetToList(ResultSet resultSet, String dataName) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        int rowCounter = 0;

        // Iterating through the result set and adding each row to a list.
        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            resultList.add(row);
            rowCounter++;
        }
        System.out.printf("[INFO] Loaded %d rows from %s.%n", rowCounter, dataName);
        return resultList;
    }

    private static List<List<Map<String, Object>>> createPartitions(List<Map<String, Object>> data, int partitionSize) {
        List<List<Map<String, Object>>> partitionList = new ArrayList<>();
        int dataIndex = 0;

        // Splitting the data into partitions of the specified size.
        while (dataIndex < data.size()) {
            partitionList.add(data.subList(dataIndex, Math.min(dataIndex + partitionSize, data.size())));
            dataIndex += partitionSize;
        }

        return partitionList;
    }

    private static Map<String, Object> findById(List<Map<String, Object>> data, String key, Object value) {
        // Iterating through the list to find a record with a matching key-value pair.
        for (Map<String, Object> record : data) {
            if (value.equals(record.get(key))) {
                return record;
            }
        }
        return null;
    }
}
