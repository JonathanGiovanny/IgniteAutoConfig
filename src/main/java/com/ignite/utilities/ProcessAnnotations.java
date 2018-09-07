package com.ignite.utilities;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
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

import com.ignite.utilities.annotations.IgniteColumn;
import com.ignite.utilities.annotations.IgniteId;
import com.ignite.utilities.annotations.IgniteTable;
import com.ignite.utilities.dto.ColumnDTO;
import com.ignite.utilities.dto.TableDTO;

public class ProcessAnnotations {

	private TableDTO tableData;

	private JdbcType jdbcType;
	private QueryEntity entity;

	private boolean isIdDeclared;
	private boolean isColumnDeclared;

	private String cacheName;

	private static final String CLASSNAME = "[ProcessNotations]";

	/**
	 * Creates the jdbcType and entity objects for Apache Ignite configuration based on the @IgniteTable, @IgniteId and @IgniteColumn notations
	 * 
	 * @param valueClass
	 *            Class with the notations
	 * @throws Exception
	 *             Message for possible notation errors or JdbcType or QueryEntity errors
	 */
	public void loadData(Class<?> valueClass) throws Exception {
		// Process @IgniteTable
		if (valueClass.isAnnotationPresent(IgniteTable.class)) {
			Annotation annotationTable = valueClass.getAnnotation(IgniteTable.class);
			IgniteTable igniteTable = (IgniteTable) annotationTable;

			cacheName = igniteTable.cacheName();
			// If Name not declared then use the class name
			String tableName = "".equals(igniteTable.name()) ? valueClass.getSimpleName().toUpperCase() : igniteTable.name().toUpperCase();

			// Load table Data into DTO
			tableData = new TableDTO();
			tableData.setCacheName(cacheName);
			tableData.setTableName(tableName);
			tableData.setTableType(valueClass);

			// JDBC Type to connect to the DB table
			jdbcType = new JdbcType();
			jdbcType.setCacheName(cacheName);
			jdbcType.setValueType(valueClass);
			jdbcType.setDatabaseTable(tableName);

			// Primary key of the table
			List<JdbcTypeField> jdbcKeys = new ArrayList<>();
			List<JdbcTypeField> jdbcValues = new ArrayList<>();

			// Entity Type to create table on cache
			entity = new QueryEntity();
			entity.setValueType(valueClass.getCanonicalName());

			Set<String> entityKeys = new TreeSet<>();
			LinkedHashMap<String, String> entityFields = new LinkedHashMap<>();

			Field[] fields = valueClass.getDeclaredFields();

			createTableSchema(fields, jdbcKeys, jdbcValues, entityKeys, entityFields);

			jdbcType.setKeyFields(jdbcKeys.toArray(new JdbcTypeField[jdbcKeys.size()]));
			jdbcType.setValueFields(jdbcValues.toArray(new JdbcTypeField[jdbcKeys.size()]));

			entity.setKeyFields(entityKeys);
			entity.setFields(entityFields);

			if (!isIdDeclared || !isColumnDeclared) {
				throw new Exception(CLASSNAME + " [loadData] @IgniteTable should have @IgniteId and at least one @IgniteColumn");
			}
		}
	}

	/**
	 * Create the ignite objects related to the table and columns mapping defined by the notations
	 * 
	 * @param fields
	 * @param jdbcKeys
	 * @param jdbcValues
	 * @param entityKeys
	 * @param entityFields
	 * @throws Exception
	 */
	private void createTableSchema(Field[] fields, List<JdbcTypeField> jdbcKeys, List<JdbcTypeField> jdbcValues, Set<String> entityKeys,
			LinkedHashMap<String, String> entityFields) throws Exception {
		// Load column Data into DTO
		List<ColumnDTO> columns = new ArrayList<>();

		// Process Fields
		for (Field field : fields) {
			boolean isKey = false;

			// If field is annotated with @IgniteId
			if (field.isAnnotationPresent(IgniteId.class)) {
				isIdDeclared = true;
				isKey = true;
			}

			// If field is annotated with @IgniteColumn
			if (field.isAnnotationPresent(IgniteColumn.class)) {
				ColumnDTO columnData = new ColumnDTO();

				Annotation annotationField = field.getAnnotation(IgniteColumn.class);
				IgniteColumn igniteField = (IgniteColumn) annotationField;

				isColumnDeclared = true;

				Class<?> type = field.getType();
				String name = field.getName();
				// If Name not declared then use the field name
				String columnName = "".equals(igniteField.name()) ? name : igniteField.name();

				columnData.setColumnName(columnName);
				columnData.setFieldName(name);
				columnData.setFieldType(type);
				
				// If is Id Key
				if (isKey) {
					columnData.setKey(true);

					jdbcType.setKeyType(type);
					jdbcKeys.add(new JdbcTypeField(getSQLType(field.getType()), columnName, type, name));

					entity.setKeyType(type.getName());
					entity.setKeyFieldName(name);

					entityKeys.add(name);

				} else {
					// Fields aside from the PK of the the table because ID it should be there
					jdbcValues.add(new JdbcTypeField(getSQLType(field.getType()), columnName, type, name));
				}

				// QueryEntity requires to map the id as well
				entityFields.put(name, type.getName());

				columns.add(columnData);
			}

			if (isKey && !isColumnDeclared) {
				throw new Exception(CLASSNAME + "[createTableSchema] @IgniteId should have also @IgniteColumn");
			}
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

	/**
	 * Get the cacheName declared on the table
	 * 
	 * @return
	 */
	public String getCacheName() {
		return cacheName;
	}

	/**
	 * Get the created JDBCType data based on the notations
	 * 
	 * @return
	 */
	public JdbcType getJDBCType() {
		return jdbcType;
	}

	/**
	 * Get the created query entity based on the notations
	 * 
	 * @return
	 */
	public QueryEntity getQueryEntity() {
		return entity;
	}

	/**
	 * Get the table data object based on TableDTO
	 * 
	 * @return
	 */
	public TableDTO getTableData() {
		return tableData;
	}
}
