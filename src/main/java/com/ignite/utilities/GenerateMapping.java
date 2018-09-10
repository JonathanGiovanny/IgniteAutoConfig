package com.ignite.utilities;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;

import com.ignite.utilities.annotations.IgniteColumn;
import com.ignite.utilities.annotations.IgniteId;
import com.ignite.utilities.dto.ColumnDTO;
import com.ignite.utilities.dto.TableDTO;

public class GenerateMapping {

	private JdbcType[] jdbcTypes;
	private Collection<QueryEntity> entities;

	public void createTableSchema(String cacheName, TableDTO tableData) throws Exception {
		// Load column Data into DTO
		List<ColumnDTO> columns = tableData.getColumns();

		// Process Fields
		for (ColumnDTO column : columns) {
			boolean isKey = column.isKey();

			// If is Id Key
			if (isKey) {
				jdbcType.setKeyType(type);
				jdbcKeys.add(new JdbcTypeField(getSQLType(column.getType()), columnName, type, name));

				entity.setKeyType(type.getName());
				entity.setKeyFieldName(name);

				entityKeys.add(name);

			} else {
				// Fields aside from the PK of the the table because ID it should be there
				jdbcValues.add(new JdbcTypeField(getSQLType(column.getType()), columnName, type, name));
			}

			// QueryEntity requires to map the id as well
			entities.put(name, type.getName());
		}
		
		// Fill columns info for tableData
		tableData.setColumns(columns);
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
		} else if (type == LocalDate.class || type == LocalDateTime.class || type == LocalTime.class) {
			return Types.TIMESTAMP;
		}
		return Types.VARCHAR;
	}

	public JdbcType[] getJdbcTypes() {
		return jdbcTypes;
	}

	public Collection<QueryEntity> getEntities() {
		return entities;
	}
}
