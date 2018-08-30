/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.revert.builder.factory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.hamcrest.Matchers;
import org.junit.Test;

import io.shardingsphere.revert.BuilderFactoryCreater;
import io.shardingsphere.revert.builder.factory.mysql.BuilderFactory;
public class MySQLBuilderFactoryTest {
    public static final String updateSQL = "update t1 set id =5,name=\"6\" where mobile='11' and name='5'";

    @Test
    public void testUpdate() {
        BuilderFactory builder = BuilderFactoryCreater.getBuilderFactory(1);
        SQLPartInfo part = builder.build(updateSQL);
        assertNotNull(part);
        assertEquals(part.getUpdateTable(),"t1");
        assertThat(part.getTableAlias(), Matchers.hasEntry("t1", "t1"));
        assertThat(part.getUpdateColumns(), Matchers.hasItems("id", "name"));
        assertEquals(part.getUpdateConditionString(),"mobile='11' and name='5'");
        assertEquals(part.getWhereParamIndexRange().size(),0);
    }
}
