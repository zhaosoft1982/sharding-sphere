package io.shardingsphere.revert;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import io.shardingsphere.core.metadata.table.ColumnMetaData;
import io.shardingsphere.core.metadata.table.TableMetaData;

public class BasicRevertEngineTest extends SnapshotEngineMultiKeyTest {
    public BasicRevertEngineTest() {
        selectSQL = "select * from t2 where id = ?";
        addValue = new Object[] { 1, "lisa", "13666666666" };
        updateParam = new Object[] { "jack", "13111111111", 1 };
        updateExpect = new Object[] { 1, "jack", "13111111111" };
        ids = new Integer[] { 1 };
        insertSQL = "insert into t2 values(?,?,?)";
        updateSQL = "update t2 t set name =?, mobile=? where t.id =?";
        deleteSQL = "delete from t2 where id =?";
    }

    public void createTableMetaData() throws Exception {
        Collection<ColumnMetaData> columnMetas = new ArrayList<>();
        columnMetas.add(new ColumnMetaData("id", "int", true));
        columnMetas.add(new ColumnMetaData("name", "varchar", false));
        columnMetas.add(new ColumnMetaData("mobile", "varchar", false));
        meta = new TableMetaData(columnMetas);
    }

    @Test
    public void insertMultiValuesTest() throws Exception {
        String sql = "insert into t2 values(?,?,?), (?,?,?)";
        Object[] params = new Object[] { 7, "haha", "11212", 8, "cool", "11215" };
        this.execDML(sql, params);
        System.out.println("------------------ after insert");

        List<List<Object>> result = this.selectData("select * from t2 where id in(?,?)", new Object[] { 7, 8 });
        check(result, params);

        SnapshotEngine engine = new SnapshotEngine(ds, sql, new Object[] { 7, 8 }, 1, meta);
        RevertContext context = engine.snapshot();
        System.out.println(context);

        engine.revert();
        System.out.println("------------------ after revert");
        result = this.selectData("select * from t2 where id in(?,?)", new Object[] { 7, 8 });
        assertEquals(result.size(), 0);
    }

    private void check(List<List<Object>> list, Object[] params) {
        int i = 0;
        for (List<Object> rowValue : list) {
            for (Object each : rowValue) {
                assertEquals(each, params[i]);
                i++;
            }
        }
    }
}
