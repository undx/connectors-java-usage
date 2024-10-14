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
package org.talend.sdk.component.standalone.internals.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.talend.sdk.component.standalone.internals.ConnectorsHandler;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Marks a class as running under Talend Component Kit context
 * and makes available ConnectorsHandler as an injection.
 */
@Target(TYPE)
@Retention(RUNTIME)
@Repeatable(WithConnectors.class)
@ExtendWith(ConnectorsHandler.class)
public @interface WithConnector {

    /**
     * @return the package containing the component(s).
     */
    String value();

    /**
     * You can force the id of the plugin instead of using component-manager auto-naming.
     * @return id of plugin's container.
     */
    String id() default "";

    /**
     * Specify plugin's provisioning mode. You can select:
     * <ul>
     *     <li><b>GAV</b> form like <code>groupId:artifactId:version</code>. </li>
     *     <li><b>JAR</b> path to the artifact relative to m2 repository like <code>org/talend/component/jdbc/1.61.0/jdbc-1.61.0.jar</code> or absolute path like <code>~/jdbc-1.61.0.jar</code></li>
     * </ul>
     * @return provisioning mode: GAV or JAR.
     */
    ProvisioningMode provision() default ProvisioningMode.GAV;

    @RequiredArgsConstructor
    enum ProvisioningMode {
        /**
         * plugin's GAV coordinates.
         */
        GAV("gav"),
        /**
         * plugin's jar path. Full path or relative to m2 repository.
         */
        JAR("jar");

        @Getter
        private final String key;
    }

    /**
     * You can isolate some packages during the test.
     * Note that in the context of a test this can have side effects so ensure to know what you
     * are doing.
     *
     * @return the package loaded from their own classloader.
     */
    String[] isolatedPackages() default {};
}
