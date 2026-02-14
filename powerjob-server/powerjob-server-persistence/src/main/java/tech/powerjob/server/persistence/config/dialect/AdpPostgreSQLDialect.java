package tech.powerjob.server.persistence.config.dialect;

import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.LongVarbinaryJdbcType;
import org.hibernate.type.descriptor.jdbc.LongVarcharJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;

import java.sql.Types;

/**
 * <a href="https://github.com/PowerJob/PowerJob/issues/750">PG数据库方言</a>
 * 使用方自行通过配置文件激活：spring.datasource.remote.hibernate.properties.hibernate.dialect=tech.powerjob.server.persistence.config.dialect.AdpPostgreSQLDialect
 *
 * @author litong0531
 * @since 2024/8/11
 */
public class AdpPostgreSQLDialect extends PostgreSQLDialect {

    @Override
    protected String columnType(int sqlTypeCode) {
        return switch (sqlTypeCode) {
            case Types.BLOB -> "bytea";
            case Types.CLOB, Types.NCLOB -> "text";
            default -> super.columnType(sqlTypeCode);
        };
    }

    @Override
    public JdbcType resolveSqlTypeDescriptor(String columnTypeName, int jdbcTypeCode, int precision, int scale, JdbcTypeRegistry jdbcTypeRegistry) {
        return switch (jdbcTypeCode) {
            case Types.CLOB -> LongVarcharJdbcType.INSTANCE;
            case Types.BLOB, Types.NCLOB -> LongVarbinaryJdbcType.INSTANCE;
            default -> super.resolveSqlTypeDescriptor(columnTypeName, jdbcTypeCode, precision, scale, jdbcTypeRegistry);
        };
    }
}
