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

import org.antlr.v4.runtime.ParserRuleContext;

import io.shardingsphere.revert.DMLType;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;

public interface SQLPartBuilder {

    /**Get sql type.
     * 
     * @return DMLType 
     */
    DMLType getType();

    /** Build sql part info.
     * 
     * @param sqlPart dml select parts
     * @param root node of syntax tree.
     */
    void build(SQLPartInfo sqlPart, ParserRuleContext root);

}
