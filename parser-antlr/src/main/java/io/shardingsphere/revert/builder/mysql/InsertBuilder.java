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

package io.shardingsphere.revert.builder.mysql;

import org.antlr.v4.runtime.ParserRuleContext;

import io.shardingsphere.revert.DMLType;
import io.shardingsphere.revert.builder.AbstractSQLPartBuilder;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;

public final class InsertBuilder extends AbstractSQLPartBuilder {
    public InsertBuilder() {
        this.setType(DMLType.INSERT);
    }

    @Override
    public void buildTableAlias(final SQLPartInfo sqlPart, final ParserRuleContext root) {
        if (root.getChildCount() > 0) {
            String tableName = root.getChild(1).getText();
            if ("into".equalsIgnoreCase(tableName)) {
                tableName = root.getChild(2).getText();
            }
            sqlPart.getTableAlias().put(tableName, tableName);
            sqlPart.setUpdateTable(tableName);
        }
    }

    protected void buildUpdateConditionString(final SQLPartInfo sqlPart, final ParserRuleContext root) {

    }
}
