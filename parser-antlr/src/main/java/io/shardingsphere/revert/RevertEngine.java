package io.shardingsphere.revert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import io.shardingsphere.revert.builder.factory.SQLPartInfo;
import io.shardingsphere.revert.builder.factory.mysql.BuilderFactory;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class RevertEngine {
	private DataSource ds;
	private String sql;
	private Object[] params;
	private Integer dbType;
	public RevertContext buildContext() throws SQLException {
		BuilderFactory factory = BuilderFactoryProducer.getBuilderFactory(dbType);
		if (factory == null) {
			throw new RuntimeException("invalid db type");
		}

		SQLPartInfo sqlPart = factory.build(sql);
		return generateConext(sqlPart);
	}

	private RevertContext generateConext(SQLPartInfo sqlPart) throws SQLException {
		RevertContext context = new RevertContext();
		context.setOriginSQL(sql);
		context.setOriginParams(params);
		if (DMLType.DELETE == sqlPart.getType() || DMLType.UPDATE == sqlPart.getType()) {
			fillSelectSql(context, sqlPart);
			fillSelectParam(context, sqlPart);
			fillSelectResult(context, sqlPart);
		}
		return context;
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
			int pos = 0;
			for (String each : sqlPart.getUpdateColumns()) {
				builder.append(each);
				if (pos < sqlPart.getUpdateColumns().size()) {
					builder.append(",");
				}
				pos++;
			}
		}
		builder.append(" from ").append(sqlPart.getUpdateTable());
		String alias = sqlPart.getTableAlias().get(sqlPart.getUpdateTable());
		if (alias != null) {
			if (!alias.equals(sqlPart.getUpdateTable())) {
				builder.append(" ").append(alias).append(" ");
			}
		}
		builder.append(" where ").append(sqlPart.getUpdateConditionString());
		context.setSelectSql(builder.toString());
	}

	private void fillSelectParam(RevertContext context, SQLPartInfo sqlPart) throws SQLException {
		if (context.getOriginParams().length > 0) {
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
		PreparedStatement ps = ds.getConnection().prepareStatement(context.getSelectSql());
		if (context.getSelectParam() != null) {
			for (int i = 0; i < context.getSelectParam().length; i++) {
				ps.setObject(i + 1, context.getSelectParam()[i]);
			}
		}

		ResultSet rs = ps.executeQuery();
		int columnCount = rs.getMetaData().getColumnCount();
		while (rs.next()) {
			Object[] rowResult = new Object[columnCount];
			context.getSelectResult().add(rowResult);
			for (int i = 1; i <= columnCount; i++) {
				rowResult[i - 1] = rs.getObject(i);
			}
		}
	}

	private void fillRevertSql(RevertContext context, SQLPartInfo sqlPart) {
		StringBuilder builder = new StringBuilder();
		if (DMLType.DELETE == sqlPart.getType()) {
			if (context.getSelectResult() != null && !context.getSelectResult().isEmpty()) {
				builder.append("insert into ").append(sqlPart.getUpdateTable()).append(" values(");
				int length = context.getSelectResult().get(0).length;
				for (Object[] params : context.getSelectResult()) {
					context.getRevertParam().add(params);
				}

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

			}
		}
	}
}
