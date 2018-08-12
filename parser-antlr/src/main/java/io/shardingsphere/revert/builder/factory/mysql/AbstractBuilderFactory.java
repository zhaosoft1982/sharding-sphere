package io.shardingsphere.revert.builder.factory.mysql;

import org.antlr.v4.runtime.ParserRuleContext;

import io.shardingsphere.revert.builder.SQLPartBuilder;
import io.shardingsphere.revert.builder.factory.SQLPartInfo;

public abstract class AbstractBuilderFactory implements BuilderFactory {

	/**
	 * @param sql
	 * @return
	 */
	protected abstract ParserRuleContext getRoot(String sql);

	/**
	 * @param root
	 * @return
	 */
	protected abstract SQLPartBuilder getBuilder(ParserRuleContext root);

	@Override
	public final SQLPartInfo build(String sql) {
		ParserRuleContext root = getRoot(sql);
		SQLPartBuilder builder = getBuilder(root);
		SQLPartInfo sqlPart = new SQLPartInfo(builder.getType(), sql);
		builder.build(sqlPart, root);
		return sqlPart;
	}

}
