package io.shardingsphere.revert;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RevertContext {
	private String originSQL;
	private Object[] originParams;
	private String selectSql;
	private List<Object[]> selectResult = new ArrayList<>();
	private Object[] selectParam;
	private String revertSQL;
	private List<Object[]> revertParam = new ArrayList<>();

}
