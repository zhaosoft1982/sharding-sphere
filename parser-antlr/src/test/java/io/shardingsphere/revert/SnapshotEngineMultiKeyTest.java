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

import org.junit.Test;

public class SnapshotEngineMultiKeyTest extends BaseSnapshotEngineTest {
    public SnapshotEngineMultiKeyTest() {
        selectSQL = "select * from t11 where id1 = ? and id2=? and id3=?";
        addValue = new Object[] { 4, 1, 1, "tom", "13666666666" };
        updateParam = new Object[] { "jim", "13999999999", 4, 1, 1 };
        updateExpect = new Object[] { 4, 1, 1 ,"jim", "13999999999"};
        ids = new Integer[] { 4, 1, 1 };
        insertSQL = "insert into t11 values(?,?,?,?,?)";
        updateSQL = "update t11  set name =?, mobile=? where id1 =? and id2=? and id3=?";
        deleteSQL = "delete from t11 where id1 =? and id2=? and id3=?";
    }

    @Test
    public void insertTest() throws Exception {
        insertData();
        checkUpdate(ids, addValue);
        System.out.println("------------------ after insert");

        SnapshotEngine engine = new SnapshotEngine(ds, insertSQL, ids, 1, meta);
        RevertContext context = engine.snapshot();
        System.out.println(context);
        engine.revert();
        checkUpdate(ids, null);
        System.out.println("------------------ after revert");
        execDML(deleteSQL, ids);
    }

    @Test
    public void updateTest() throws Exception {
        insertData();

        SnapshotEngine engine = new SnapshotEngine(ds, updateSQL, updateParam, 1, meta);
        RevertContext context = engine.snapshot();
        System.out.println(context);
        updateData();
        engine.revert();
        System.out.println("------------------ after revert");
        checkUpdate(ids, addValue);
        
        deleteData();
    }

    @Test
    public void deleteTest() throws Exception {
        SnapshotEngine engine = new SnapshotEngine(ds, deleteSQL, (Object[]) ids, 1, meta);
        RevertContext context = engine.snapshot();
        deleteData();
        System.out.println(context);
        engine.revert();
        checkUpdate(ids, null);
        
        System.out.println("------------------ after revert");
        deleteData();
    }
}
