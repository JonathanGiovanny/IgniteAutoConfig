package com.ignite.utilities;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;

import com.ignite.utilities.dto.ColumnDTO;
import com.ignite.utilities.dto.TableDTO;

public class GenerateMapping {

	private JdbcType jdbcType;
	private QueryEntity queryEntity;

	public void createTableSchema(String cacheName, TableDTO tableData) throws Exception {
		jdbcType = new JdbcType();
		queryEntity = new QueryEntity();
		queryEntity.setValueType(tableData.getTableType().getCanonicalName());

		// JDBC Type to connect to the DB table
		jdbcType = new JdbcType();
		jdbcType.setCacheName(cacheName);
		jdbcType.setValueType(tableData.getTableType());
		jdbcType.setDatabaseTable(tableData.getTableName());

		// Entity Type to create table on cache
		queryEntity = new QueryEntity();
		queryEntity.setValueType(tableData.getTableType().getCanonicalName());

		// Load column Data into DTO
		List<ColumnDTO> columns = tableData.getColumns();

		// List of the keys for the table
		Set<String> entityKeys = new TreeSet<>();
		// List of the fields for that table
		LinkedHashMap<String, String> entityFields = new LinkedHashMap<>();

		// List of keys for the table
		List<JdbcTypeField> jdbcKeys = new ArrayList<>();
		// List of values for the table
		List<JdbcTypeField> jdbcValues = new ArrayList<>();

		// Process Fields
		for (ColumnDTO column : columns) {
			boolean isKey = column.isKey();

			// If is Id Key
			if (isKey) {
				jdbcType.setKeyType(column.getFieldType());
				jdbcKeys.add(
						new JdbcTypeField(getSQLType(column.getFieldType()), column.getColumnName(), column.getFieldType(), column.getFieldName()));

				queryEntity.setKeyType(column.getFieldType().getName());
				queryEntity.setKeyFieldName(column.getFieldName());

				entityKeys.add(column.getFieldName());
			}

			// JdbcValues maps the object data
			jdbcValues.add(
					new JdbcTypeField(getSQLType(column.getFieldType()), column.getColumnName(), column.getFieldType(), column.getFieldName()));

			// QueryEntity maps the table column data
			entityFields.put(column.getFieldName(), column.getFieldType().getName());
		}

		jdbcType.setKeyFields(jdbcKeys.toArray(new JdbcTypeField[jdbcKeys.size()]));
		jdbcType.setValueFields(jdbcValues.toArray(new JdbcTypeField[jdbcKeys.size()]));

		queryEntity.setKeyFields(entityKeys);
		queryEntity.setFields(entityFields);
	}

	/**
	 * Get the SQL type for the main table types * Default Type is varchar
	 * 
	 * @param type
	 * @return
	 */
	private int getSQLType(Class<?> type) {
		if (type == Long.class) {
			return Types.BIGINT;
		} else if (type == Integer.class) {
			return Types.INTEGER;
		} else if (type == Double.class) {
			return Types.DOUBLE;
		} else if (type == String.class) {
			return Types.VARCHAR;
		} else if (type == LocalDate.class || type == LocalDateTime.class || type == LocalTime.class || type == Timestamp.class) {
			return Types.TIMESTAMP;
		}
		return Types.VARCHAR;
	}

	/**
	 * Get the JdbcTypes for the cacheConfiguration
	 * 
	 * @return
	 */
	public JdbcType getJdbcType() {
		return jdbcType;
	}

	/**
	 * Get the QueryEntities for the cacheConfiguration
	 * 
	 * @return
	 */
	public QueryEntity getQueryEntity() {
		return queryEntity;
	}
}
