package io.shardingsphere.revert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class RevertContext {
	private String originSQL;
	private Object[] originParams;
	private String selectSql;
	private List<Map<String,Object>> selectResult = new ArrayList<>();
	private Object[] selectParam;
	private String revertSQL;
	private List<Collection<Object>> revertParam = new ArrayList<>();

}
