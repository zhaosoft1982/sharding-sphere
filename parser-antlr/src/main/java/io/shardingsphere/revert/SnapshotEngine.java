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

package io.shardingsphere.revert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import io.shardingsphere.core.metadata.table.ColumnMetaData;
import io.shardingsphere.core.metadata.table.TableMetaData;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;
import io.shardingsphere.revert.builder.factory.mysql.BuilderFactory;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@RequiredArgsConstructor
@Setter
@Getter
@ToString
public final class SnapshotEngine {

    private final DataSource ds;

    private final String sql;

    private final Object[] params;

    private final Integer dbType;

    private final TableMetaData tableMeta;

    private List<String> keys;

    private RevertContext context;

    /**Get table key columns.
     * 
     */
    private void getKeyColumns() {
        keys = new ArrayList<>();
        for (ColumnMetaData each : tableMeta.getColumnMetaData()) {
            if (each.isPrimaryKey()) {
                keys.add(each.getColumnName());
            }
        }
    }

    /**Generate snapshot before sql execution.
     * 
     * @return Revert context info 
     * @throws SQLException failed to execute sql throw exception
     */
    public RevertContext snapshot() throws SQLException {
        if (null == keys) {
            getKeyColumns();
        }

        BuilderFactory factory = BuilderFactoryCreater.getBuilderFactory(dbType);
        if (null == factory) {
            throw new RuntimeException("invalid db type");
        }

        SQLPartInfo sqlPart = factory.build(sql);
        generateConext(sqlPart);
        return context;
    }

    /**Generate revert context.
     * 
     * @param sqlPart origin sql parts
     * @return Revert context info 
     * @throws SQLException failed to execute sql throw exception
     */
    private void generateConext(final SQLPartInfo sqlPart) throws SQLException {
        context = new RevertContext(sql, params);
        if (DMLType.DELETE == sqlPart.getType() || DMLType.UPDATE == sqlPart.getType()) {
            fillSelectSql(context, sqlPart);
            fillSelectParam(context, sqlPart);
            fillSelectResult(context, sqlPart);
        }

        fillRevertSql(context, sqlPart);
    }

    /**Get and fill select sql.
     * 
     * @param context revert context info
     * @param sqlPart origin sql parts
     */
    private void fillSelectSql(final RevertContext context, final SQLPartInfo sqlPart) {
        StringBuilder builder = new StringBuilder();

        if (DMLType.DELETE != sqlPart.getType() && DMLType.UPDATE != sqlPart.getType()) {
            throw new RuntimeException("invalid DML operation");
        }

        builder.append("select  ");
        if (DMLType.DELETE == sqlPart.getType()) {
            builder.append(" * ");
        } else if (DMLType.UPDATE == sqlPart.getType()) {
            fillColumnForUpdate(builder, sqlPart);
        }

        builder.append(" from ").append(sqlPart.getUpdateTable());
        
        String alias = getTableAlias(sqlPart);

        if (alias != null && !alias.equals(sqlPart.getUpdateTable())) {
            builder.append(" ").append(alias).append(" ");
        }

        if (sqlPart.getUpdateConditionString() != null && !sqlPart.getUpdateConditionString().isEmpty()) {
            builder.append(" where ").append(sqlPart.getUpdateConditionString());
        }

        context.setSelectSQL(builder.toString());
    }
    
    /**Get table alias.
     * 
     * @param sqlPart origin sql parts
     * @return table alias
     */
    private String getTableAlias(final SQLPartInfo sqlPart) {
        for (Map.Entry<String, String> each : sqlPart.getTableAlias().entrySet()) {
            if (sqlPart.getUpdateTable().equals(each.getValue())) {
                return each.getKey();
            }
        }
        return null;
    }
    
    /**Append select column for update.
     * 
     * @param builder select sql builder
     * @param sqlPart origin sql parts
     */
    private void fillColumnForUpdate(final StringBuilder builder, final SQLPartInfo sqlPart) {
        for (String each : sqlPart.getUpdateColumns()) {
            int dotPos = each.indexOf('.');
            String realColumnName = null;
            if (dotPos > 0) {
                realColumnName = each.substring(dotPos + 1);
            } else {
                realColumnName = each;
            }

            if (!keys.contains(realColumnName)) {
                builder.append(each);
                builder.append(",");
            }
        }

        int pos = 0;
        for (String each : keys) {
            builder.append(each);
            if (pos < keys.size() - 1) {
                builder.append(",");
            }

            pos++;
        }

        if (keys.isEmpty()) {
            char lastChar = builder.charAt(builder.length() - 1);
            if (lastChar == ',') {
                builder.replace(builder.length() - 1, builder.length() - 1, "");
            }
        }
    }

    /**Get and fill select sql param.
     * 
     * @param context revert context info
     * @param sqlPart origin sql parts
     * @throws SQLException failed to execute sql throw exception
     */
    private void fillSelectParam(final RevertContext context, final SQLPartInfo sqlPart) throws SQLException {
        if (null == context.getOriginParams() || context.getOriginParams().length <= 0) {
            return;
        }
        
        if (sqlPart.getWhereParamIndexRange().isEmpty()) {
            return;
        }
        
        int start = sqlPart.getWhereParamIndexRange().get(0);
        int end = context.getOriginParams().length - 1;
        if (sqlPart.getWhereParamIndexRange().size() > 1) {
            end = sqlPart.getWhereParamIndexRange().get(1);
        }

        Object[] selectParam = new Object[end - start];
        int pos = 0;
        for (int i = start; i < end; i++) {
            selectParam[pos++] = context.getOriginParams()[i];
        }
        context.setSelectParam(selectParam);
    }

    /**Fill query result into revert context.
     * 
     * @param context revert context
     * @param sqlPart origin sql parts
     * @throws SQLException failed to execute sql throw exception
     */
    private void fillSelectResult(final RevertContext context, final SQLPartInfo sqlPart) throws SQLException {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(context.getSelectSQL());
            if (context.getSelectParam() != null) {
                for (int i = 0; i < context.getSelectParam().length; i++) {
                    ps.setObject(i + 1, context.getSelectParam()[i]);
                }
            }

            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> rowResultMap = new LinkedHashMap<>();
                context.getSelectResult().add(rowResultMap);
                for (int i = 1; i <= columnCount; i++) {
                    rowResultMap.put(rsMeta.getColumnName(i), rs.getObject(i));
                }
            }
        } finally {
            closePsAndConn(conn, ps);
        }
    }

    /**Get and fill revert sql.
     * 
     * @param context revert context
     * @param sqlPart origin sql parts
     */
    private void fillRevertSql(final RevertContext context, final SQLPartInfo sqlPart) {
        if (context.getSelectResult() == null || context.getSelectResult().isEmpty()) {
            return;
        }
        
        StringBuilder builder = new StringBuilder();
        if (DMLType.DELETE == sqlPart.getType()) {
            fillRevertSqlForDelete(context, sqlPart);
        } else if (DMLType.UPDATE == sqlPart.getType()) {
            fillRevertSqlForUpdate(context, sqlPart);
        } else if (DMLType.INSERT == sqlPart.getType()) {
            fillRevertSqlForInsert(context, sqlPart);
        }

        context.setRevertSQL(builder.toString());
    }
    
    /**Get and fill revert sql for delete.
     * 
     * @param context revert context
     * @param sqlPart origin sql parts
     */
    private void fillRevertSqlForDelete(final RevertContext context, final SQLPartInfo sqlPart) {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(sqlPart.getUpdateTable()).append(" values(");

        for (Map<String, Object> each : context.getSelectResult()) {
            context.getRevertParam().add(each.values());
        }

        int length = context.getSelectResult().get(0).size();
        for (int i = 0; i < length; i++) {
            builder.append("?");
            if (i < length - 1) {
                builder.append(",");
            }
        }

        builder.append(" )");
        context.setRevertSQL(builder.toString());
    }
    
    /**Get and fill revert sql for update.
     * 
     * @param context revert context
     * @param sqlPart origin sql parts
     */
    private void fillRevertSqlForUpdate(final RevertContext context, final SQLPartInfo sqlPart) {
        StringBuilder builder = new StringBuilder();
        builder.append("update ").append(sqlPart.getUpdateTable()).append(" set ");
        int pos = 0;
        for (String updateColumn : sqlPart.getUpdateColumns()) {
            builder.append(updateColumn).append(" = ?");
            if (pos < sqlPart.getUpdateColumns().size() - 1) {
                builder.append(",");
            }
            pos++;
        }

        builder.append(" where ");
        pos = 0;
        for (String key : keys) {
            if (pos > 0) {
                builder.append(" and ");
            }
            builder.append(key).append(" = ? ");
            pos++;

        }

        List<Map<String, Object>> selectResult = context.getSelectResult();
        for (Map<String, Object> rowResult : selectResult) {
            List<Object> rowRevertParam = new ArrayList<>();
            context.getRevertParam().add(rowRevertParam);

            for (String updateColumn : sqlPart.getUpdateColumns()) {
                rowRevertParam.add(rowResult.get(updateColumn));
            }

            for (String key : keys) {
                rowRevertParam.add(rowResult.get(key));
            }
        }
        context.setRevertSQL(builder.toString());
    }
    
    /**Get and fill revert sql for insert.
     * 
     * @param context revert context
     * @param sqlPart origin sql parts
     */
    private void fillRevertSqlForInsert(final RevertContext context, final SQLPartInfo sqlPart) {
        StringBuilder builder = new StringBuilder();
        builder.append(" delete from ").append(sqlPart.getUpdateTable());
        builder.append(" where ");
        int pos = 0;
        for (Object key : keys) {
            if (pos > 0) {
                builder.append(" and ");
            }

            builder.append(key).append(" = ?");
            pos++;
        }

        pos = 0;
        for (Object param : params) {
            if (pos % keys.size() == 0) {
                context.getRevertParam().add(new ArrayList<>());
            }
            List<Object> rowRevertParam = (List<Object>) context.getRevertParam()
                    .get(context.getRevertParam().size() - 1);
            rowRevertParam.add(param);
            pos++;
        }

        context.setRevertSQL(builder.toString());
    }

    /**Revert data to snapshot.
     * 
     * @throws SQLException failed to execute sql throw exception
     */
    public void revert() throws SQLException {
        for (Collection<Object> rowParam : context.getRevertParam()) {
            PreparedStatement ps = null;
            Connection conn = null;
            try {
                conn = ds.getConnection();
                ps = conn.prepareStatement(context.getRevertSQL());
                if (rowParam != null) {
                    Iterator<Object> it = rowParam.iterator();
                    int i = 0;
                    while (it.hasNext()) {
                        ps.setObject(i + 1, it.next());
                        i++;
                    }
                }

                ps.execute();
            } finally {
                closePsAndConn(conn, ps);
            }
        }
    }

    /**Close sql statement and connection.
     * 
     * @param conn sql connection
     * @param ps sql prepared statement
     */
    private void closePsAndConn(final Connection conn, final PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
