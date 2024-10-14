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
package org.talend.sdk.component.standalone.internals;

import static java.lang.Character.toUpperCase;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.Locale.ROOT;
import static java.util.Optional.ofNullable;
import static lombok.AccessLevel.PRIVATE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.JsonBuilderFactory;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbConfig;
import javax.json.spi.JsonProvider;

import org.apache.xbean.finder.filter.Filter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.dependencies.maven.MvnCoordinateToFileConverter;
import org.talend.sdk.component.runtime.base.Lifecycle;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ContainerComponentRegistry;
import org.talend.sdk.component.runtime.manager.ParameterMeta;
import org.talend.sdk.component.runtime.manager.configuration.ConfigurationMapper;
import org.talend.sdk.component.runtime.manager.json.PreComputedJsonpProvider;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.parameterenricher.BaseParameterEnricher;
import org.talend.sdk.component.runtime.manager.service.LocalConfigurationService;
import org.talend.sdk.component.runtime.manager.xbean.registry.EnrichedPropertyEditorRegistry;
import org.talend.sdk.component.runtime.output.Processor;
import org.talend.sdk.component.runtime.record.RecordConverters;
import org.talend.sdk.component.runtime.standalone.DriverRunner;
import org.talend.sdk.component.standalone.internals.Connectors.Connector;
import org.talend.sdk.component.standalone.internals.annotations.WithConnector;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectorsHandler {

    protected static final Local<State> STATE = ConnectorsHandler.loadStateHolder();

    private final ThreadLocal<PreState> initState = ThreadLocal.withInitial(PreState::new);

    protected String packageName;

    protected Collection<Connector> connectors;

    protected Collection<String> isolatedPackages;

    private static Local<State> loadStateHolder() {
        switch (System.getProperty("talend.component.connectors.handler.state", "thread").toLowerCase(ROOT)) {
        case "static":
            return new Local.StaticImpl<>();
        default:
            return new Local.ThreadLocalImpl<>();
        }
    }

    public ConnectorsHandler() {
        final WithConnector[] annotations = getClass().getAnnotationsByType(WithConnector.class);
        final Connectors annotatedConnectors = Connectors.fromAnnotations(annotations);
        isolatedPackages = annotatedConnectors.getAllIsolatedPackages();
        connectors = annotatedConnectors.getPlugins().values();
        start();
        injectServices(this);
    }

    public static <T> Map<String, String> configurationByExample(final T instance, final String prefix) {
        return ConnectorsHandler.configurationByExample().forInstance(instance).withPrefix(prefix).configured().toMap();
    }

    public static ByExample configurationByExample() {
        return new ByExample();
    }

    public <T> T injectServices(final T instance) {
        if (null == instance) {
            return null;
        }
        getPlugins().forEach(plugin -> {
            final Map<Class<?>, Object> services = asManager()
                    .findPlugin(plugin)
                    .orElseThrow(() -> new IllegalArgumentException("cant find plugin '" + plugin + "'"))
                    .get(ComponentManager.AllServices.class)
                    .getServices();
            ((Injector) services.get(Injector.class)).inject(instance);
        });

        return instance;
    }

    public List<String> getConnectors() {
        return asManager().getContainer().getPluginsList();
    }


    public EmbeddedComponentManager start() {
        final EmbeddedComponentManager embeddedComponentManager = new EmbeddedComponentManager(connectors) {

            @Override
            protected boolean isContainerClass(final Filter filter, final String name) {
                if (null == name) {
                    return super.isContainerClass(filter, null);
                }
                return (null == isolatedPackages || isolatedPackages.stream().noneMatch(name::startsWith))
                        && super.isContainerClass(filter, name);
            }

            @Override
            public void close() {
                try {
                    final State state = ConnectorsHandler.STATE.get();
                    if (null != state && null != state.jsonb) {
                        try {
                            state.jsonb.close();
                        } catch (final Exception e) {
                            // no-op: not important
                        }
                    }
                    ConnectorsHandler.STATE.remove();
                    initState.remove();
                } finally {
                    super.close();
                }
            }
        };

        ConnectorsHandler.STATE
                .set(new State(embeddedComponentManager, new CopyOnWriteArrayList<>(), initState.get().emitter, null,
                               null, null, null));
        return embeddedComponentManager;
    }

    public Mapper createMapper(final Class<?> componentType, final Object configuration) {
        return create(Mapper.class, componentType, configuration);
    }

    public Processor createProcessor(final Class<?> componentType, final Object configuration) {
        return create(Processor.class, componentType, configuration);
    }

    public DriverRunner createDriverRunner(final Class<?> componentType, final Object configuration) {
        return create(DriverRunner.class, componentType, configuration);
    }

    private <C, T, A> A create(final Class<A> api, final Class<T> componentType, final C configuration) {
        final ComponentFamilyMeta.BaseMeta<? extends Lifecycle> meta = findMeta(componentType);
        return api
                .cast(meta
                              .getInstantiator()
                              .apply(null == configuration || meta.getParameterMetas().get().isEmpty() ? emptyMap()
                                             : ConnectorsHandler.configurationByExample(configuration, meta
                                      .getParameterMetas()
                                      .get()
                                      .stream()
                                      .filter(p -> p.getName().equals(p.getPath()))
                                      .findFirst()
                                      .map(p -> p.getName() + '.')
                                      .orElseThrow(() -> new IllegalArgumentException(
                                              "Didn't find any option and therefore "
                                                      + "can't convert the configuration instance to a configuration")))));
    }

    public Iterator<Record> getInputIterator(final Mapper mapper) {
        return new InputIterator(mapper);
    }

    private <T> ComponentFamilyMeta.BaseMeta<? extends Lifecycle> findMeta(final Class<T> componentType) {
        return asManager()
                .find(c -> c.get(ContainerComponentRegistry.class).getComponents().values().stream())
                .flatMap(f -> Stream
                        .of(f.getProcessors().values().stream(), f.getPartitionMappers().values().stream(),
                            f.getDriverRunners().values().stream())
                        .flatMap(t -> t))
                .filter(m -> m.getType().getName().equals(componentType.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No component " + componentType));
    }

    public ComponentManager asManager() {
        return ConnectorsHandler.STATE.get().manager;
    }

    public Set<String> getPlugins() {
        return new HashSet<>(((EmbeddedComponentManager) asManager()).plugins);
    }

    public void resetState() {
        final State state = ConnectorsHandler.STATE.get();
        if (null == state) {
            ConnectorsHandler.STATE.remove();
        } else {
            state.collector.clear();
            state.emitter = emptyIterator();
        }
    }

    private String getSinglePlugin() {
        return Optional
                .of(((EmbeddedComponentManager) asManager()).plugins/* sorted */)
                .filter(c -> !c.isEmpty())
                .map(c -> c.iterator().next())
                .orElseThrow(() -> new IllegalStateException("No component plugin found"));
    }

    static class PreState {

        Iterator<?> emitter;
    }

    @AllArgsConstructor
    protected static class State {

        final ComponentManager manager;

        final Collection<Object> collector;

        final RecordConverters.MappingMetaRegistry registry = new RecordConverters.MappingMetaRegistry();

        Iterator<?> emitter;

        volatile Jsonb jsonb;

        volatile JsonProvider jsonProvider;

        volatile JsonBuilderFactory jsonBuilderFactory;

        volatile RecordBuilderFactory recordBuilderFactory;

        synchronized Jsonb jsonb() {
            if (null == jsonb) {
                jsonb = manager
                        .getJsonbProvider()
                        .create()
                        .withProvider(new PreComputedJsonpProvider("test", manager.getJsonpProvider(),
                                                                   manager.getJsonpParserFactory(), manager.getJsonpWriterFactory(),
                                                                   manager.getJsonpBuilderFactory(), manager.getJsonpGeneratorFactory(),
                                                                   manager.getJsonpReaderFactory())) // reuses the same memory buffers
                        .withConfig(new JsonbConfig().setProperty("johnzon.cdi.activated", false))
                        .build();
            }
            return jsonb;
        }

        synchronized JsonProvider jsonProvider() {
            if (null == jsonProvider) {
                jsonProvider = manager.getJsonpProvider();
            }
            return jsonProvider;
        }

        synchronized JsonBuilderFactory jsonBuilderFactory() {
            if (null == jsonBuilderFactory) {
                jsonBuilderFactory = manager.getJsonpBuilderFactory();
            }
            return jsonBuilderFactory;
        }

        synchronized RecordBuilderFactory recordBuilderFactory() {
            if (null == recordBuilderFactory) {
                recordBuilderFactory = manager.getRecordBuilderFactoryProvider().apply("test");
            }
            return recordBuilderFactory;
        }
    }

    public static class EmbeddedComponentManager extends ComponentManager {

        private final ComponentManager oldInstance;

        private final MvnCoordinateToFileConverter mvnCoordinateToFileConverter = new MvnCoordinateToFileConverter();

        private final List<String> plugins = new ArrayList<>();

        private EmbeddedComponentManager(final Collection<Connector> connectors) {
            super(findM2());
            connectors.stream().forEach(connector -> {
                final String name = connector.getId().isEmpty() ? "" : connector.getId();
                final String path = connector.getProvisioning().equals("gav")
                        ? mvnCoordinateToFileConverter.toArtifact(connector.getArtifact()).toPath()
                        : connector.getArtifact();
                log.warn("[EmbeddedComponentManager] name: {} path: {}", name, path);
                plugins.add(name.isEmpty() ? addPlugin(path) : addPlugin(name, path));
            });
            oldInstance = contextualInstance().get();
            contextualInstance().set(this);
        }

        @Override
        public void close() {
            try {
                super.close();
            } finally {
                contextualInstance().compareAndSet(this, oldInstance);
            }
        }

        @Override
        protected boolean isContainerClass(final Filter filter, final String name) {
            // embedded mode (no plugin structure) so just run with all classes in parent classloader
            return true;
        }
    }

    interface Local<T> {

        void set(T value);

        T get();

        void remove();

        class StaticImpl<T> implements Local<T> {

            private final AtomicReference<T> state = new AtomicReference<>();

            @Override
            public void set(final T value) {
                state.set(value);
            }

            @Override
            public T get() {
                return state.get();
            }

            @Override
            public void remove() {
                state.set(null);
            }
        }

        class ThreadLocalImpl<T> implements Local<T> {

            private final ThreadLocal<T> threadLocal = new ThreadLocal<>();

            @Override
            public void set(final T value) {
                threadLocal.set(value);
            }

            @Override
            public T get() {
                return threadLocal.get();
            }

            @Override
            public void remove() {
                threadLocal.remove();
            }
        }
    }

    public static class InputIterator implements Iterator<Record>, AutoCloseable {

        private final Mapper mapper;

        private final Input input;

        private Object next;

        public InputIterator(final Mapper mapper) {
            this.mapper = mapper;
            mapper.start();
            input = mapper.create();
            input.start();
        }

        @Override
        public boolean hasNext() {
            next = input.next();
            final boolean hasNext = null != next;
            // stop lifecycle
            if (!hasNext) {
                input.stop();
                mapper.stop();
            }
            //
            return hasNext;
        }

        @Override
        public Record next() {
            return (Record) next;
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException("#close()");
        }
    }

    @NoArgsConstructor(access = PRIVATE)
    public static class ByExample {

        private String prefix;

        private Object instance;

        public ByExample withPrefix(final String prefix) {
            this.prefix = prefix;
            return this;
        }

        public <T> ByExample forInstance(final T instance) {
            this.instance = instance;
            return this;
        }

        public ConfigurationByExample configured() {
            return new ConfigurationByExample(this);
        }
    }

    private static class SimpleParameterModelService extends ParameterModelService {

        public SimpleParameterModelService() {
            super(new EnrichedPropertyEditorRegistry());
        }

        private ParameterMeta build(final String name, final String prefix, final Type genericType,
                                    final Annotation[] annotations, final Collection<String> i18nPackages) {
            return buildParameter(name, prefix, null, genericType, annotations, i18nPackages, false,
                                  new BaseParameterEnricher.Context(new LocalConfigurationService(emptyList(), "test")));
        }
    }


    @AllArgsConstructor(access = PRIVATE)
    public static class ConfigurationByExample {

        private static final ConfigurationMapper CONFIGURATION_MAPPER = new ConfigurationMapper();

        private final ByExample byExample;

        private static String encode(final String source) {
            final byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
            final ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length);
            boolean changed = false;
            for (final byte rawByte : bytes) {
                byte b = rawByte;
                if (0 > b) {
                    b += 256;
                }
                if (('a' <= b && 'z' >= b || 'A' <= b && 'Z' >= b) || ('0' <= b && '9' >= b) || '-' == b || '.' == b
                        || '_' == b || '~' == b) {
                    bos.write(b);
                } else {
                    bos.write('%');
                    char hex1 = toUpperCase(Character.forDigit((b >> 4) & 0xF, 16));
                    char hex2 = toUpperCase(Character.forDigit(b & 0xF, 16));
                    bos.write(hex1);
                    bos.write(hex2);
                    changed = true;
                }
            }
            return changed ? bos.toString(StandardCharsets.UTF_8) : source;
        }

        public Map<String, String> toMap() {
            if (null == byExample.instance) {
                return emptyMap();
            }
            final String usedPrefix = ofNullable(byExample.prefix).orElse("configuration.");
            final ParameterMeta params = new SimpleParameterModelService()
                    .build(usedPrefix, usedPrefix, byExample.instance.getClass(), new Annotation[0],
                           new ArrayList<>(singletonList(byExample.instance.getClass().getPackage().getName())));
            return CONFIGURATION_MAPPER.map(params.getNestedParameters(), byExample.instance);
        }

        public String toQueryString() {
            return toMap()
                    .entrySet()
                    .stream()
                    .sorted(comparing(Map.Entry::getKey))
                    .map(entry -> entry.getKey() + "=" + encode(entry.getValue()))
                    .collect(Collectors.joining("&"));
        }
    }
}