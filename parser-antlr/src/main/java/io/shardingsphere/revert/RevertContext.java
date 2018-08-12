package io.shardingsphere.revert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@RequiredArgsConstructor
@Setter
@Getter
@ToString
public final class RevertContext {
	
	private final String originSQL;
	
	private final Object[] originParams;
	
	private String selectSQL;
	
	private final List<Map<String,Object>> selectResult = new ArrayList<>();
	
	private Object[] selectParam;
	
	private String revertSQL;
	
	private final List<Collection<Object>> revertParam = new ArrayList<>();
}
