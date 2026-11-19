Big Data Practical 6: Fraud Detection with Apache Flink
=======================================================

Name: Arvind Yadav
Roll no: 31011123065
Class: LYCS

Aim
---

Flink State Lab: Implement a Flink application that detects "Fraudulent Transactions" (two large
transactions within 1 minute) using KeyedProcessFunction.

How it works
------------

The transactions are grouped by customer with keyBy(). For each customer, FraudDetector (a
KeyedProcessFunction) remembers the last transaction of ₹10,000 or more in a ValueState. When
another transaction of ₹10,000 or more arrives within 60 seconds of it, a fraud alert is printed.
Transactions below ₹10,000 are ignored.

  Time    Customer   Amount   Result
  ------- ---------- -------- ---------------------------------
  10 s    C101       15,000   Large; remembered
  20 s    C202       5,000    Small; ignored
  45 s    C101       20,000   35 s after ₹15,000: FRAUD ALERT
  50 s    C202       12,000   Large; remembered
  55 s    C101       3,000    Small; ignored
  90 s    C101       18,000   45 s after ₹20,000: FRAUD ALERT
  130 s   C202       14,000   80 s after ₹12,000: no alert

Files
-----

  File                                   Purpose
  -------------------------------------- ------------------------------------------------------------
  pom.xml                                Maven project file: Java 17 and the Flink 1.19.2 libraries
  src/main/java/FraudDetectionJob.java   The Flink program

Step 1. Install Java 17
-----------------------

1.  Download Temurin 17 from https://adoptium.net and install it.
2.  In the installer, turn on Set JAVA_HOME variable and Add to PATH.
3.  Open a new terminal and check:
        java -version

    It should start with openjdk version "17.

Step 2. Install Maven
---------------------

1.  On https://maven.apache.org/download.cgi, click apache-maven-3.10.1-bin.zip (the link in the
    Binary zip archive row).
2.  Right-click the zip in Downloads, choose Extract All, enter C:\Tools, and click Extract.
3.  Check that C:\Tools\apache-maven-3.10.1\bin\mvn.cmd exists, then add Maven to PATH:
        [Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";C:\Tools\apache-maven-3.10.1\bin", "User")
4.  Close VS Code completely, open it again, and check:
        mvn -version

    It should print Apache Maven 3.10.1 and your Java version.

Step 3. Install the Java extension in VS Code
---------------------------------------------

Click Extensions in the left sidebar, search Extension Pack for Java, and install the one by
Microsoft.

Step 4. Create the project
--------------------------

Option A, download it from this repo:

    git clone https://github.com/show-star11/al6.git
    cd al6

Option B, create it by hand:

    mkdir practical-6
    cd practical-6
    mkdir src\main\java

The folder must look exactly like this:

    practical-6
    ├── pom.xml
    └── src
        └── main
            └── java
                └── FraudDetectionJob.java

Open the folder in VS Code with File > Open Folder.

Step 5. pom.xml (in the main project folder)
--------------------------------------------

    <project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
    https://maven.apache.org/xsd/maven-4.0.0.xsd">

      <modelVersion>4.0.0</modelVersion>

      <groupId>com.example</groupId>
      <artifactId>flink-practical</artifactId>
      <version>1.0-SNAPSHOT</version>

      <properties>
        <maven.compiler.release>17</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <flink.version>1.19.2</flink.version>
      </properties>

      <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
         </dependency>
      </dependencies>

      <build>
         <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
            </plugin>
         </plugins>
      </build>

    </project>

Step 6. FraudDetectionJob.java (in src\main\java)
-------------------------------------------------

The file name must be exactly FraudDetectionJob.java, with the same capital letters.

    import org.apache.flink.api.common.state.ValueState;
    import org.apache.flink.api.common.state.ValueStateDescriptor;
    import org.apache.flink.api.common.typeinfo.Types;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
    import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
    import org.apache.flink.util.Collector;


    import java.io.Serializable;
    import java.time.Instant;


    public class FraudDetectionJob {
    public static class Transaction implements Serializable {
        public String customerId;
        public double amount;
        public long timestamp;


        public Transaction() {}


        public Transaction(String customerId, double amount, long timestamp) {
            this.customerId = customerId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }


    public static class FraudAlert implements Serializable {
        public String customerId;
        public double previousAmount;
        public double currentAmount;
        public long gapMs;


        public FraudAlert() {}


        public FraudAlert(String customerId, double previousAmount,
                     double currentAmount, long gapMs) {
            this.customerId = customerId;
            this.previousAmount = previousAmount;
            this.currentAmount = currentAmount;
            this.gapMs = gapMs;
        }
        @Override
        public String toString() {
            return "FRAUD ALERT: Customer=" + customerId
                + ", Previous=₹" + previousAmount
                + ", Current=₹" + currentAmount
                + ", Gap=" + gapMs / 1000 + " seconds";
        }
    }


    public static class FraudDetector
            extends KeyedProcessFunction<String, Transaction, FraudAlert> {


        private transient ValueState<Transaction> previousTransaction;


        @Override
        public void open(
              org.apache.flink.configuration.Configuration parameters) {


            ValueStateDescriptor<Transaction> descriptor =
                new ValueStateDescriptor<>(
                     "previous-large-transaction",
                     Types.POJO(Transaction.class));


            previousTransaction = getRuntimeContext().getState(descriptor);
        }


        @Override
        public void processElement(
                Transaction tx,
                Context ctx,
                Collector<FraudAlert> out) throws Exception {


            // Only consider transactions of ₹10,000 or more.
            if (tx.amount < 10000) {
                return;
            }


            Transaction previous = previousTransaction.value();


            if (previous != null) {
                long gap = tx.timestamp - previous.timestamp;


                if (gap >= 0 && gap <= 60000) {
                    out.collect(new FraudAlert(
                          tx.customerId,
                          previous.amount,
                          tx.amount,
                          gap));
                }
            }


            // Update the state for this customer.
            previousTransaction.update(tx);
        }
    }


    public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env =
                 StreamExecutionEnvironment.getExecutionEnvironment();


            long base = Instant.parse("2026-10-19T10:00:00Z")
                 .toEpochMilli();


            DataStream<Transaction> transactions = env.fromData(
                 new Transaction("C101", 15000, base + 10000),
                 new Transaction("C202", 5000, base + 20000),
                 new Transaction("C101", 20000, base + 45000),
                 new Transaction("C202", 12000, base + 50000),
                 new Transaction("C101", 3000, base + 55000),
                 new Transaction("C101", 18000, base + 90000),
                 new Transaction("C202", 14000, base + 130000)
            );


            transactions
                 .keyBy(tx -> tx.customerId)
                 .process(new FraudDetector())
                 .print();


            env.execute("Fraud Detection Job");
        }
    }

Step 7. Download Flink and compile
----------------------------------

In the VS Code terminal, inside the project folder (the one with pom.xml):

    mvn clean compile

The first run takes a few minutes. It should end with BUILD SUCCESS.

Step 8. Save the list of Flink libraries
----------------------------------------

    mvn dependency:build-classpath "-Dmdep.outputFile=classpath.txt"

This should also end with BUILD SUCCESS, and a file classpath.txt appears in the folder.

Step 9. Run the program
-----------------------

To show ₹ correctly, set the terminal to UTF-8 first, then run:

    chcp 65001
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $cp = Get-Content .\classpath.txt
    java "-Dstdout.encoding=UTF-8" "-Dsun.stdout.encoding=UTF-8" -cp "target\classes;$cp" FraudDetectionJob

Expected output
---------------

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
    7> FRAUD ALERT: Customer=C101, Previous=₹15000.0, Current=₹20000.0, Gap=35 seconds
    7> FRAUD ALERT: Customer=C101, Previous=₹20000.0, Current=₹18000.0, Gap=45 seconds

The SLF4J lines are normal and only mean that no logging library is set up. 7> is the number of the
Flink task that printed the line and can be a different number on another laptop.

If something goes wrong
-----------------------

  Error                                                 Fix
  ----------------------------------------------------- --------------------------------------------------------------------------------------------------------------------------
  'java' is not recognized                              Install JDK 17 with Add to PATH turned on, then restart VS Code
  'mvn' is not recognized                               Check that C:\Tools\apache-maven-3.10.1\bin\mvn.cmd exists, run the PATH command from Step 2 again, then restart VS Code
  JAVA_HOME ... not defined correctly                   Run setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17..." with your JDK folder name, then reopen the terminal
  Could not resolve dependencies                        Check the internet connection and run mvn clean compile again
  Could not find or load main class FraudDetectionJob   Check that the file is in src\main\java, run mvn clean compile again, and run from the folder with pom.xml
  NoClassDefFoundError: org/apache/flink/...            Run Step 8 again, and run all lines of Step 9 in the same terminal
  ? instead of ₹                                        Run the chcp and [Console] lines from Step 9 before the java command

Conclusion
----------

A Flink application was built with Maven that groups transactions by customer with keyBy() and
checks them with a KeyedProcessFunction. A ValueState stores the last large transaction of each
customer, and an alert is produced when two transactions of ₹10,000 or more come within 1 minute.
The program found the two fraudulent pairs of customer C101 and ignored the small transactions and
C202's transactions that were more than 1 minute apart.
