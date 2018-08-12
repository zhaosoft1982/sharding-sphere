package io.shardingsphere.revert.builder.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.shardingsphere.revert.DMLType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@RequiredArgsConstructor
@Setter
@Getter
@ToString
public final class SQLPartInfo {
	private final DMLType type;
	
	private final String sql;
	
	private List<String> updateColumns = new ArrayList<>();
	
	private String updateTable;
	
	private Map<String, String> tableAlias = new HashMap<>();
	
	private String updateConditionString;
	
	private List<Integer> whereParamIndexRange = new ArrayList<>();

}
