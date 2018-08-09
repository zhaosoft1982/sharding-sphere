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

public final class RevertEngine {
	private DataSource ds;
	private String sql;
	private Object[] params;
	private Integer dbType;
	private TableMetaData tableMeta;
	private List<String> keys;
	private RevertContext context;

	public RevertEngine(DataSource ds, String sql, Object[] params, Integer dbType, TableMetaData tableMeta) {
		this.ds = ds;
		this.sql = sql;
		this.params = params;
		this.dbType = dbType;
		this.tableMeta = tableMeta;
		getKeyColumns();
	}

	private void getKeyColumns() {
		keys = new ArrayList<>();
		for (ColumnMetaData each : tableMeta.getColumnMetaData()) {
			if (each.isPrimaryKey()) {
				keys.add(each.getColumnName());
			}
		}
	}

	public RevertContext snapshot() throws SQLException {
		BuilderFactory factory = BuilderFactoryProducer.getBuilderFactory(dbType);
		if (factory == null) {
			throw new RuntimeException("invalid db type");
		}

		SQLPartInfo sqlPart = factory.build(sql);
		generateConext(sqlPart);
		return context;
	}

	private void generateConext(SQLPartInfo sqlPart) throws SQLException {
		context = new RevertContext();
		context.setOriginSQL(sql);
		context.setOriginParams(params);
		if (DMLType.DELETE == sqlPart.getType() || DMLType.UPDATE == sqlPart.getType()) {
			fillSelectSql(context, sqlPart);
			fillSelectParam(context, sqlPart);
			fillSelectResult(context, sqlPart);
		}

		fillRevertSql(context, sqlPart);
	}

	private void fillSelectSql(RevertContext context, SQLPartInfo sqlPart) {
		StringBuilder builder = new StringBuilder();

		if (DMLType.DELETE != sqlPart.getType() && DMLType.UPDATE != sqlPart.getType()) {
			throw new RuntimeException("invalid DML operation");
		}

		builder.append("select  ");
		if (DMLType.DELETE == sqlPart.getType()) {
			builder.append(" * ");
		} else if (DMLType.UPDATE == sqlPart.getType()) {
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
		
		builder.append(" from ").append(sqlPart.getUpdateTable());
		String alias = null;
		for(Map.Entry<String, String> entry : sqlPart.getTableAlias().entrySet()) {
			if(sqlPart.getUpdateTable().equals(entry.getValue())) {
				alias = entry.getKey();
				break;
			}
		}
		
		if (alias != null) {
			if (!alias.equals(sqlPart.getUpdateTable())) {
				builder.append(" ").append(alias).append(" ");
			}
		}

		if (sqlPart.getUpdateConditionString() != null && !sqlPart.getUpdateConditionString().isEmpty()) {
			builder.append(" where ").append(sqlPart.getUpdateConditionString());
		}

		context.setSelectSql(builder.toString());
	}

	private void fillSelectParam(RevertContext context, SQLPartInfo sqlPart) throws SQLException {
		if (null != context.getOriginParams() && context.getOriginParams().length > 0) {
			if (sqlPart.getWhereParamIndexRange().size() > 0) {
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
		}
	}

	private void fillSelectResult(RevertContext context, SQLPartInfo sqlPart) throws SQLException {
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			conn = ds.getConnection();
			ps = conn.prepareStatement(context.getSelectSql());
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

	private void fillRevertSql(RevertContext context, SQLPartInfo sqlPart) {
		StringBuilder builder = new StringBuilder();
		if (DMLType.DELETE == sqlPart.getType()) {
			if (context.getSelectResult() != null && !context.getSelectResult().isEmpty()) {
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
			}
		} else if (DMLType.UPDATE == sqlPart.getType()) {
			if (context.getSelectResult() != null && !context.getSelectResult().isEmpty()) {
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

			}
		} else if (DMLType.INSERT == sqlPart.getType()) {
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
		}

		context.setRevertSQL(builder.toString());
	}

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

	private void closePsAndConn(Connection conn, PreparedStatement ps) {
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
