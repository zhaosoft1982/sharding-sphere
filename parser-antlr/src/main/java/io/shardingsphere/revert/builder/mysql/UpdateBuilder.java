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

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.AliasContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.ColumnNameContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.SetClauseContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.TableFactorContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.TableNameContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.TableReferencesContext;
import io.shardingsphere.revert.DMLType;
import io.shardingsphere.revert.builder.AbstractSQLPartBuilder;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;
import io.shardingsphere.utils.TreeUtils;

public final class UpdateBuilder extends AbstractSQLPartBuilder {
    public UpdateBuilder() {
        this.setType(DMLType.DELETE);
    }

    @Override
    public void buildUpdateColumns(final SQLPartInfo sqlPart, final ParserRuleContext root) {
        SetClauseContext setCtx = (SetClauseContext) TreeUtils.getFirstDescendant(root, SetClauseContext.class, false);
        if (null != setCtx) {
            List<ParseTree> columns = TreeUtils.getAllDescendantByClass(setCtx, ColumnNameContext.class);
            for (ParseTree each : columns) {
                sqlPart.getUpdateColumns().add(each.getText());
            }
        }
    }

    @Override
    public void buildTableAlias(final SQLPartInfo sqlPart, final ParserRuleContext root) {
        TableReferencesContext tableReferencesCtx = (TableReferencesContext) TreeUtils.getFirstDescendant(root,
                TableReferencesContext.class, false);
        if (null != tableReferencesCtx) {
            List<ParseTree> tableFactorCtxs = TreeUtils.getAllDescendantByClass(tableReferencesCtx,
                    TableFactorContext.class);
            for (ParseTree each : tableFactorCtxs) {
                TableNameContext tableNameCtx = (TableNameContext) TreeUtils.getFirstDescendant(each,
                        TableNameContext.class, false);

                AliasContext aliasCtx = (AliasContext) TreeUtils.getFirstDescendant(each, AliasContext.class, false);
                String tableName = tableNameCtx.getText();
                String alias = tableName;
                if (null != aliasCtx) {
                    alias = aliasCtx.getText();
                }

                if ((null == tableName || "".equals(tableName)) && null != alias && !"".equals(alias)) {
                    tableName = alias;
                }

                if (null != tableName && !"".equals(tableName)) {
                    sqlPart.getTableAlias().put(alias, tableName);
                    sqlPart.setUpdateTable(tableName);
                }
            }
        }
    }
}
