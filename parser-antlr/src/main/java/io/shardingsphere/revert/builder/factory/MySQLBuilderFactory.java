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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import io.shardingsphere.parser.antlr.mysql.MySQLDMLLexer;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.DeleteContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.InsertContext;
import io.shardingsphere.parser.antlr.mysql.MySQLDMLParser.UpdateContext;
import io.shardingsphere.revert.builder.SQLPartBuilder;
import io.shardingsphere.revert.builder.factory.mysql.AbstractBuilderFactory;
import io.shardingsphere.revert.builder.mysql.DeleteBuilder;
import io.shardingsphere.revert.builder.mysql.InsertBuilder;
import io.shardingsphere.revert.builder.mysql.UpdateBuilder;

public class MySQLBuilderFactory extends AbstractBuilderFactory {

    /**
     * Get root node of syntax tree.
     * 
     * @param sql dml sql statement
     * @return root node of syntax tree
     */
    @Override
    protected ParserRuleContext getRoot(final String sql) {
        CharStream input = CharStreams.fromString(sql);
        MySQLDMLLexer lexer = new MySQLDMLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        MySQLDMLParser parser = new MySQLDMLParser(tokens);
        ParseTree tree = parser.execute();
        if (tree != null) {
            if (tree instanceof ParserRuleContext) {
                return (ParserRuleContext) tree.getChild(0);
            }
        }

        return null;
    }

    /**
     * Get dml builder.
     * 
     * @param root node of syntax tree
     * @return sql part info
     */
    public SQLPartBuilder getBuilder(final ParserRuleContext root) {
        if (root instanceof InsertContext) {
            return new InsertBuilder();
        } else if (root instanceof UpdateContext) {
            return new UpdateBuilder();
        } else if (root instanceof DeleteContext) {
            return new DeleteBuilder();
        } else {
            throw new RuntimeException("sql revert does not support the sql");
        }
    }
}
