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
package org.talend.connectors.demo;


import static java.util.stream.Collectors.toList;
import static org.talend.connectors.demo.JdbcDemo.PLUGIN;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.talend.components.jdbc.configuration.InputTableNameConfig;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.connectors.internal.ConnectorsHandler;
import org.talend.connectors.internal.WithConnector;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.classloader.ConfigurableClassLoader;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ComponentManager.AllServices;
import org.talend.sdk.component.runtime.manager.asm.ProxyGenerator;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.xbean.registry.EnrichedPropertyEditorRegistry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WithConnector(value = "jdbc", jarLocation = PLUGIN)
public class JdbcDemo extends ConnectorsHandler {

    public static final String JDBC_URL = "jdbc:mysql://localhost:3306/mydatabase";

    public static final String JDBC_USER = "user";

    public static final String JDBC_PASS = "password";

    public static final String JDBC_DB = "mydatabase";

    public static final String PLUGIN = "/home/undx/.m2/repository/org/talend/components/jdbc/1.61.0/jdbc-1.61.0.jar";


    final Map<String, String> config = new HashMap<>();

    ComponentManager manager;

    @Service
    UIActionService uiService;

    public JdbcDemo() {
        manager = asManager();
        initConfigurations();
    }

    protected void initConfigurations() {
        log.warn("[initConfigurations]");

        //if (manager.getContainer().findAll().isEmpty()) {            manager.addPlugin(PLUGIN);        }

        JdbcConnection connection = new JdbcConnection();
        connection.setJdbcUrl(JDBC_URL);
        connection.setUserId(JDBC_USER);
        connection.setPassword(JDBC_PASS);

        JdbcConfiguration conf = new JdbcConfiguration();
        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setDataset(null);

        log.warn("[initConfigurations] {}", uiService);
        log.warn("[initConfigurations] {}", conf.getDrivers());

        config.put("configuration.dataSet.connection.dbType", "MySQL");
        config.put("configuration.dataSet.connection.setRawUrl", "false");
        config.put("configuration.dataSet.connection.host", "localhost");
        config.put("configuration.dataSet.connection.port", "3306");
        config.put("configuration.dataSet.connection.database", "mydatabase");
        config.put("configuration.dataSet.connection.userId", "user");
        config.put("configuration.dataSet.connection.password", "password");
        config.put("configuration.dataSet.tableName", "users");
        config.put("configuration.dataSet.connection.defineProtocol", "false");
        config.put("configuration.dataSet.connection.connectionTimeOut", "30");
        config.put("configuration.dataSet.connection.connectionValidationTimeOut", "10");
        config.put("configuration.dataSet.advancedCommon.fetchSize", "1000");
        config.put("configuration.dataSet.__version", "3");
        config.put("configuration.dataSet.connection.__version", "3");
    }


    public void displayTable() {
        final Mapper driver = manager.findMapper("Jdbc", "TableNameInput", 3, config)
                .orElseThrow(() -> new IllegalArgumentException("Can't find Jdbc#TableNameInput"));
        driver.start();
        final List<Mapper> splittedMappers = driver.split(driver.assess());
        for (Mapper m : splittedMappers) {
            m.start();
            Input input = m.create();
            input.start();
            Record r = (Record) input.next();
            while (r != null) {
                log.info("[displayTable] Id: {} Name: {} Active: {}.", r.getInt("id"), r.getString("name"), r.getBoolean("active"));
                r = (Record) input.next();
            }
            input.stop();
            m.stop();
        }
        driver.stop();
    }

    private Object unwrap(final Object instance) {
        if (instance.getClass().getName().endsWith("$$TalendServiceProxy")) {
            try {
                Object delegate = new ProxyGenerator().getHandler(instance).getDelegate();
                return delegate;
            } catch (final IllegalStateException nsfe) {
                // no-op
                log.error("[unwrap]", nsfe);
            }
        }
        return instance;
    }

    public void executeServices() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Container container = manager.getContainer().find("jdbc").orElseThrow();

        final Map<Class<?>, Object> services = container.get(AllServices.class).getServices();
        services.keySet().forEach(c -> log.info("[executeServices] service: {}", c.getCanonicalName()));

        Object svc = services.entrySet().stream().filter(s -> s.getKey().getCanonicalName().contains("UIActionService"))
                .findFirst().get().getValue();
        log.warn("[executeServices] got service {} methods {}", svc.getClass().getName(), Arrays.stream(svc.getClass()
                                                                                                                .getMethods())
                .map(m -> m.getName())
                .collect(toList()));

        final Injector injector = Injector.class.cast(services.get(Injector.class));
//        log.warn("[executeServices] injector {} {}", injector, injector.inject(svc));

        //injectServices(this);
        //injector.inject(this);

        final Method method = svc.getClass().getMethod("loadSupportedDataBaseTypes");
        Values vals = Values.class.cast(method.invoke(svc));

        log.warn("[executeServices] values {}", vals);

        // $$TalendServiceProxy
        //        UIActionService service = UIActionService.class.cast(unwrap(svc));
        //UIActionService.class.cast(services.entrySet().stream().filter(s -> s.getKey().getCanonicalName().contains("UIActionService")).findFirst().get().getValue());
        //UIActionService service = UIActionService.class.cast(services.get(org.talend.components.jdbc.service.UIActionService.class));
        log.warn("[executeServices] SupportedDataBaseTypes {}", uiService.loadSupportedDataBaseTypes());
    }

    public void callService() throws Exception {
        ClassLoader backupClassLoader = Thread.currentThread().getContextClassLoader();

        Container jdbc = manager.getContainer().find("jdbc").get();
        ConfigurableClassLoader jdbcClassLoader = jdbc.getLoader();
        Thread.currentThread().setContextClassLoader(jdbcClassLoader);
        try {
            ClassLoader newClassLoader = Thread.currentThread().getContextClassLoader();

            Map<Class<?>, Object> services = manager.findPlugin("jdbc").get()
                    .get(AllServices.class).getServices();

            Class<?> inputTableNameConfigClass = jdbcClassLoader.loadClass("org.talend.components.jdbc.configuration.InputTableNameConfig");

            EnrichedPropertyEditorRegistry propertyEditorRegistry = new EnrichedPropertyEditorRegistry();
            ParameterModelService parameterModelService = new ParameterModelService(propertyEditorRegistry);
            ReflectionService reflectionService = new ReflectionService(parameterModelService, propertyEditorRegistry);
            BiFunction<String, Map<String, Object>, InputTableNameConfig> configFactory = (BiFunction<String, Map<String, Object>, org.talend.components.jdbc.configuration.InputTableNameConfig>) reflectionService.createObjectFactory(inputTableNameConfigClass);
            Map<String, Object> collect = config.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> (Object) e.getValue()));

            org.talend.components.jdbc.configuration.InputTableNameConfig inputTableNameConfig = configFactory.apply("configuration", collect);

            Class<?> uiActionServiceClass = jdbcClassLoader.loadClass("org.talend.components.jdbc.service.UIActionService");
            Object uiActionService = services.get(uiActionServiceClass);

            Class<?> jdbcConnectionClass = jdbcClassLoader.loadClass("org.talend.components.jdbc.datastore.JdbcConnection");
            Method getTableFromDatabase = uiActionService.getClass()
                    .getMethod("getTableFromDatabase", jdbcConnectionClass);

            Class<?> suggestionValueClass = jdbcClassLoader.loadClass("org.talend.sdk.component.api.service.completion.SuggestionValues");
            suggestionValueClass.cast(getTableFromDatabase.invoke(inputTableNameConfig.getDataSet().getConnection()));

            /*
            Iterator<Item> tableIterator = tableFromDatabase.getItems().iterator();
            while (tableIterator.hasNext()) {
                SuggestionValues.Item table = tableIterator.next();
                System.out.printf("- %s : %s\n" + table.getId(), table.getLabel());
            }
            */

        } finally {
            Thread.currentThread().setContextClassLoader(backupClassLoader);
        }
    }

    public static void main(String[] args) {
        try {
            JdbcDemo demo = new JdbcDemo();
            demo.displayTable();
            demo.executeServices();
        } catch (Exception e) {
            log.error("[main]", e);
        }
    }

}
