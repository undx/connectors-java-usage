/*
 * Copyright (C) 2006-2024 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package org.talend.sdk.component.standalone.demo;


import static java.util.stream.Collectors.toList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import org.talend.components.jdbc.configuration.InputTableNameConfig;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.input.TableNameInputEmitter;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.components.jdbc.service.JdbcService.JdbcDatasource;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.components.jdbc.service.UIActionService.TableInfo;
import org.talend.components.jms.data.JMSVersion;
import org.talend.components.jms.dataset.JMSDataset;
import org.talend.components.jms.datastore.JMSDatastore;
import org.talend.components.jms.service.JMSService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.completion.Values.Item;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.standalone.internals.ConnectorsHandler;
import org.talend.sdk.component.standalone.internals.annotations.WithConnector;
import org.talend.sdk.component.standalone.internals.annotations.WithConnector.ProvisioningMode;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WithConnector(id = "jdbc-patched", provision = ProvisioningMode.JAR, value = JdbcDemo.LOCATION)
@WithConnector("org.talend.components:jms-connector:1.62.0-SNAPSHOT")
@WithConnector("org.talend.components:jms-connector:1.62.0-SNAPSHOT") // same hash so will be discarded...
public class JdbcDemo extends ConnectorsHandler {

    /* CHANGE ME */
    public static final String DB_TYPE = "MySQL";

    /* CHANGE ME */
    public static final String JDBC_URL = "jdbc:mysql://localhost:3306/mydatabase";

    /* CHANGE ME */
    public static final String JDBC_USER = "user";

    /* CHANGE ME */
    public static final String JDBC_PASS = "password";

    public static final String LOCATION = "/home/undx/jdbc-1.61.0.jar";

    public static final String LOCATION_REL = "org/talend/components/jdbc/1.61.0/jdbc-1.61.0.jar";

    public static final String GAV = "org.talend.components:jdbc:1.61.0";

    public static final String JAR_LOCATION = LOCATION_REL;

    JdbcConnection connection;

    TableNameDataset dataset;

    JdbcDatasource datasource;

    InputTableNameConfig tableconfig;

    JMSDataset jmsDataset;

    JMSDatastore jmsDatastore;

    /**
     * jdbc connector services
     */
    @Service
    UIActionService uiService;

    @Service
    JdbcService jdbcService;

    @Service
    JMSService jmsService;

    @Service
    org.talend.components.jms.service.UIActionService jmsUiService;

    public static void main(String[] args) {
        try {
            JdbcDemo demo = new JdbcDemo();
            log.warn("[main] plugins registred: {}", demo.getConnectors());
            demo.executeServices();
            demo.displayManuallyTable();
            demo.displayTableWithIterator();
            demo.useDatasource();
        } catch (Exception e) {
            log.error("[main]", e);
        }
    }

    public JdbcDemo() {
        setup();
        log.warn("[JdbcDemo] m2: {}", System.getProperty("talend.component.manager.m2.repository"));
    }

    private void setup() {
        connection = new JdbcConnection();
        connection.setDbType(JdbcDemo.DB_TYPE);
        connection.setJdbcUrl(JdbcDemo.JDBC_URL);
        connection.setUserId(JdbcDemo.JDBC_USER);
        connection.setPassword(JdbcDemo.JDBC_PASS);
        // use jdbcService to create datasource...
        datasource = jdbcService.createDataSource(connection);
        log.warn("[initConfigurations] datasource driver id: {} ", datasource.getDriverId());
        // dataset
        dataset = new TableNameDataset();
        dataset.setConnection(connection);
        dataset.setTableName("users");
        // tableInput
        tableconfig = new InputTableNameConfig();
        tableconfig.setDataSet(dataset);
        //
        // JMS
        //
        jmsDatastore = new JMSDatastore();
        jmsDatastore.setProviderId("activemq");
        jmsDatastore.setUserName("user");
        jmsDatastore.setPassword("pass");
        jmsDatastore.setServerURL("jms://localhost");
        jmsDatastore.setJmsVersion(JMSVersion.JMS_2);
        jmsDataset = new JMSDataset();
        jmsDataset.setDatastore(jmsDatastore);
        jmsDataset.setDestination("topic");
    }

    public void displayManuallyTable() {
        final Mapper mapper = createMapper(TableNameInputEmitter.class, tableconfig);
        mapper.start();
        final List<Mapper> splittedMappers = mapper.split(mapper.assess());
        for (Mapper m : splittedMappers) {
            m.start();
            Input input = m.create();
            input.start();
            Record r = (Record) input.next();
            while (null != r) {
                log.info("[displayyManuallyTable] Id: {} Name: {} Active: {}.",
                                  r.getInt("id"), r.getString("name"), r.getBoolean("active"));
                r = (Record) input.next();
            }
            input.stop();
            m.stop();
        }
        mapper.stop();
    }

    public void displayTableWithIterator() {
        Iterator<Record> iterator = getInputIterator(createMapper(TableNameInputEmitter.class, tableconfig));
        Record r;
        while (iterator.hasNext()) {
            r = iterator.next();
            log.info("[displayTableWithIterator] Id: {} Name: {} Active: {}.",
                              r.getInt("id"), r.getString("name"), r.getBoolean("active"));
        }
    }

    public void useDatasource() throws SQLException {
        Connection sqlConnection = datasource.getConnection();
        log.warn("[useDatasource] schema: {}", sqlConnection.getSchema());
        log.warn("[useDatasource] catalog: {}", sqlConnection.getCatalog());
        log.warn("[useDatasource] client: {}", sqlConnection.getClientInfo());
        log.warn("[useDatasource] meta: {}", sqlConnection.getMetaData().getTypeInfo());

        final Statement stm = sqlConnection.createStatement();
        stm.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), active BOOLEAN)");

        stm.execute("DROP TABLE IF EXISTS test");
        stm.execute("CREATE TABLE test (id INT PRIMARY KEY NOT NULL, nom VARCHAR(100))");
        stm.executeUpdate("INSERT INTO test VALUES(1, 'eg')");
        stm.executeUpdate("INSERT INTO test VALUES(2, 'yp')");
        stm.executeUpdate("INSERT INTO test VALUES(3, 'mb')");
        final ResultSet result = stm.executeQuery("SELECT id, nom FROM test");
        while (result.next()) {
            log.warn("[useDatasource] query: {} {} ", result.getInt("id"), result.getString("nom"));
        }
        stm.execute("DROP TABLE test");
        stm.close();
    }

    public void executeServices() throws SQLException {
        // supported db
        final Values dbTypes = uiService.loadSupportedDataBaseTypes();
        log.warn("[executeServices] SupportedDataBaseTypes: {}", dbTypes.getItems().stream().map(Item::getId)
                .collect(toList()));
        // display all tables in database
        final List<TableInfo> tables = uiService.listTables(connection);
        log.warn("[executeServices] Available tables: {}", tables.stream()
                .map(t -> t.getName() + "(" + t.getType() + ")").collect(toList()));
        // guess schema for all tables defined in database
        tables.forEach(tableInfo -> {
            dataset.setTableName(tableInfo.getName());
            final Schema schema = uiService.guessSchema(dataset);
            log.warn("[executeServices] `{}' table schema: {} ", tableInfo.getName(), schema);
        });

        // jms service test
        log.error("[executeServices] JMS Providers {}", jmsService.getProviders().stream().map(p -> p.getDisplayName())
                .collect(toList()));
        log.error("[executeServices] JMS Brokers {}", jmsUiService.listSupportedBroker().getItems().stream()
                .map(i -> i.getId()).collect(toList()));
        log.error("[executeServices] JMS Healthcheck {}", jmsUiService.healthCheck(jmsDatastore).toString());
    }

}
