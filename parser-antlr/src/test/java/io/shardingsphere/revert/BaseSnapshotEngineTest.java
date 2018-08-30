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

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.shardingsphere.core.metadata.table.ColumnMetaData;
import io.shardingsphere.core.metadata.table.TableMetaData;

public abstract class BaseSnapshotEngineTest {
    protected static DataSource ds;
    protected static Connection conn;
    protected TableMetaData meta;
    protected String selectSQL;

    protected Object[] addValue;
    protected Object[] updateParam;
    protected Object[] updateExpect;
    protected Integer[] ids;
    protected String insertSQL;
    protected String updateSQL;
    protected String deleteSQL;

    @BeforeClass
    public static void setup() throws Exception {
        getConnection();
    }

    @AfterClass
    public static void destroy() throws Exception {
        if (null != conn) {
            conn.close();
        }

//        if (null != ds) {
//            ds.close();
//        }
    }

    public static void getConnection() throws Exception {
        Properties properties = new Properties();
        InputStream inStream = SnapshotEngineMultiKeyTest.class.getClassLoader()
                .getResourceAsStream("hikari.properties");
        properties.load(inStream);
        System.err.println(properties);
        ds = new HikariDataSource(new HikariConfig(properties));
        conn = ds.getConnection();
    }

    @Before
    public void createTableMetaData() throws Exception {
        Collection<ColumnMetaData> columnMetas = new ArrayList<>();
        columnMetas.add(new ColumnMetaData("id1", "int", true));
        columnMetas.add(new ColumnMetaData("id2", "int", true));
        columnMetas.add(new ColumnMetaData("id3", "int", true));
        columnMetas.add(new ColumnMetaData("name", "varchar", false));
        columnMetas.add(new ColumnMetaData("mobile", "varchar", false));

        meta = new TableMetaData(columnMetas);
    }

    protected void checkUpdate(Object[] params, Object[] exptected) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(selectSQL);
        if (null != params) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        }

        ResultSet rs = ps.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        boolean hasNext = rs.next();
        if (hasNext && exptected != null) {
            for (int i = 1; i <= columnCount; i++) {
                assertEquals(rs.getObject(i), exptected[i - 1]);
            }
        }

        if (null == exptected) {
            assertEquals(hasNext, false);
        }

        ps.close();
    }

    protected void checkList(List<List<Object>> list1, List<List<Object>> list2) throws SQLException {
        assertEquals(list1.size(), list2.size());
        int j = 0;
        for (List<Object> rowValue : list1) {
            List<Object> rowValueAfterRevert = list2.get(j);
            for (int i = 0; i < 3; i++) {
                assertEquals(rowValue.get(i), rowValueAfterRevert.get(i));
            }
            j++;
        }
    }

    protected void execDML(String sql, Object[] params) throws Exception {
        PreparedStatement ps = conn.prepareStatement(sql);
        int pos = 1;
        if (null != params) {
            for (Object param : params) {
                ps.setObject(pos++, param);
            }
        }

        ps.executeUpdate();
        ps.close();
    }

    protected void insertData() throws Exception {
        execDML(insertSQL, addValue);
        System.out.println("------------------ check insert");
        checkUpdate(ids, addValue);
    }

    protected void updateData() throws Exception {
        execDML(updateSQL, updateParam);
        System.out.println("------------------ check update");
        checkUpdate(ids, updateExpect);
    }

    protected void deleteData() throws Exception {
        execDML(deleteSQL, ids);
        System.out.println("------------------ check delete");
        checkUpdate(ids, null);
    }

    protected List<List<Object>> selectData(String sql, Object[] params) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        if (null != params) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        }
        ResultSet rs = ps.executeQuery();

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        List<List<Object>> ret = new LinkedList<>();
        while (rs.next()) {
            List<Object> rowValue = new LinkedList<>();
            for (int i = 1; i <= columnCount; i++) {
                rowValue.add(rs.getObject(i));
            }
            ret.add(rowValue);
        }
        return ret;
    }
}
