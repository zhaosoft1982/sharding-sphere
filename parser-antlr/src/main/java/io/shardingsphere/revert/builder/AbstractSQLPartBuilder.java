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

package io.shardingsphere.revert.builder;

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.WhereClauseContext;
import io.shardingsphere.revert.DMLType;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;
import io.shardingsphere.utils.TreeUtils;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AbstractSQLPartBuilder implements SQLPartBuilder {
    private DMLType type;

    /** Build sql part info.
     * 
     * @param sqlPart dml select parts
     * @param root node of syntax tree
     */
    @Override
    public void build(final SQLPartInfo sqlPart, final ParserRuleContext root) {
        buildUpdateColumns(sqlPart, root);
        buildUpdateTable(sqlPart, root);
        buildTableAlias(sqlPart, root);
        buildUpdateConditionString(sqlPart, root);
    }

    /** Build update column.
     * 
     * @param sqlPart dml select parts
     * @param root node of syntax tree.
     */
    protected void buildUpdateColumns(final SQLPartInfo sqlPart, final ParserRuleContext root) {

    }

    /** Build update table.
     * 
     * @param sqlPart dml select parts
     * @param root node of syntax tree.
     */
    protected void buildUpdateTable(final SQLPartInfo sqlPart, final ParserRuleContext root) {

    }

    /** Build table alias.
     * 
     * @param sqlPart dml select parts
     * @param root node of syntax tree.
     */
    protected void buildTableAlias(final SQLPartInfo sqlPart, final ParserRuleContext root) {

    }

    /** Build update condition string.
     * 
     * @param sqlPart dml select parts
     * @param root node of syntax tree.
     */
    protected void buildUpdateConditionString(final SQLPartInfo sqlPart, final ParserRuleContext root) {
        WhereClauseContext where = (WhereClauseContext) TreeUtils.getFirstDescendant(root, WhereClauseContext.class,
                false);

        if (null != where) {
            ParserRuleContext exprCtx = (ParserRuleContext) where.getChild(1);
            String expr = sqlPart.getSql().substring(exprCtx.getStart().getStartIndex(),
                    exprCtx.getStop().getStopIndex() + 1);
            sqlPart.setUpdateConditionString(expr);
        }

        List<TerminalNode> allParams = TreeUtils.getAllTerminalByType(root, MySQLDMLParser.QUESTION);
        int paramCount = allParams.size();
        if (allParams.size() > 0) {
            List<TerminalNode> whereParams = TreeUtils.getAllTerminalByType(where, MySQLDMLParser.QUESTION);
            int whereParamCount = 0;
            if (where != null) {
                whereParamCount = whereParams.size();
            }

            sqlPart.getWhereParamIndexRange().add(paramCount - whereParamCount);
            sqlPart.getWhereParamIndexRange().add(paramCount);
        }
    }
}
