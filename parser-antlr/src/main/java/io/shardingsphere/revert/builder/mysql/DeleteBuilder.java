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

import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.FromSingleContext;
import io.shardingsphere.revert.DMLType;
import io.shardingsphere.revert.builder.AbstractSQLPartBuilder;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;
import io.shardingsphere.utils.TreeUtils;

public final class DeleteBuilder extends AbstractSQLPartBuilder {
    public DeleteBuilder() {
        this.setType(DMLType.DELETE);
    }

    @Override
    public void buildTableAlias(final SQLPartInfo sqlPart, final ParserRuleContext root) {
        String tableName = getTableFromSingle(root);
        sqlPart.getTableAlias().put(tableName, tableName);
        sqlPart.setUpdateTable(tableName);
    }

    /**Get table name from root node.
     * 
     * @param root node of syntax tree
     * @return table name
     */
    private String getTableFromSingle(final ParserRuleContext root) {
        FromSingleContext singleCtx = (FromSingleContext) TreeUtils.getFirstDescendant(root, FromSingleContext.class,
                false);
        if (null != singleCtx) {
            if (singleCtx.getChildCount() > 1) {
                return singleCtx.getChild(1).getText();
            }
        }

        return null;
    }
}
