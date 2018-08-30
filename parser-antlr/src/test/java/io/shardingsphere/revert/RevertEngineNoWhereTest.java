package io.shardingsphere.revert;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import io.shardingsphere.core.metadata.table.ColumnMetaData;
import io.shardingsphere.core.metadata.table.TableMetaData;

public class RevertEngineNoWhereTest extends BaseSnapshotEngineTest {
    public RevertEngineNoWhereTest() {
        selectSQL = "select * from t2 order by name";
        updateParam = new Object[] { "jim", "13999999999" };
        updateSQL = "update t2 set name =?, mobile=?";
        deleteSQL = "delete from t2";
    }

    public void createTableMetaData() throws Exception {
        Collection<ColumnMetaData> columnMetas = new ArrayList<>();
        columnMetas.add(new ColumnMetaData("id", "int", true));
        columnMetas.add(new ColumnMetaData("name", "varchar", false));
        columnMetas.add(new ColumnMetaData("mobile", "varchar", false));
        updateExpect = new Object[] { 4, 1, 1, "jim", "13999999999" };

        meta = new TableMetaData(columnMetas);
    }

    @Test
    public void updateTest() throws Exception {
        System.out.println("------------------ before update");
        List<List<Object>> dataInDb = selectData(selectSQL, null);
        SnapshotEngine engine = new SnapshotEngine(ds, updateSQL, updateParam, 1, meta);
        RevertContext context = engine.snapshot();
        System.out.println(context);

        this.execDML(updateSQL, updateParam);
        List<List<Object>> dataInDbAfterUpdate = selectData(selectSQL, null);
        System.out.println("------------------ after update");
        checkUpdate(dataInDbAfterUpdate);

        engine.revert();
        System.out.println("------------------ after revert");
        List<List<Object>> dataInDbAfterRevert = selectData(selectSQL, null);
        checkList(dataInDb, dataInDbAfterRevert);
    }

    @Test
    public void deleteTest() throws Exception {
        List<List<Object>> dataInDb = selectData(selectSQL, null);
        SnapshotEngine engine = new SnapshotEngine(ds, deleteSQL, null, 1, meta);
        RevertContext context = engine.snapshot();
        System.out.println(context);

        System.out.println("------------------  delete");
        this.execDML(deleteSQL, null);
        this.checkUpdate(null, null);
        System.out.println("------------------ after delete");

        engine.revert();
        System.out.println("------------------ after revert");
        List<List<Object>> dataInDbAfterRevert = selectData(selectSQL, null);
        checkList(dataInDb, dataInDbAfterRevert);
    }

    private void checkUpdate(List<List<Object>> dataInDbAfterUpdate) throws SQLException {
        for (List<Object> rowValue : dataInDbAfterUpdate) {
            for (int i = 1; i < 3; i++) {
                assertEquals(rowValue.get(i), updateParam[i - 1]);
            }
        }
    }
}
