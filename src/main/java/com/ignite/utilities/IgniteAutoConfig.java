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

public class IgniteAutoConfig {

	private static JdbcType jdbcType;
	private static QueryEntity entity;

	private static boolean isIdDeclared;
	private static boolean isColumnDeclared;

	/**
	 * Get the created JDBCType data based on the notations
	 * @return
	 */
	public static JdbcType getJDBCType() {
		return jdbcType;
	}

	/**
	 * Get the created query entity based on the notations
	 * @return
	 */
	public static QueryEntity getQueryEntity() {
		return entity;
	}
	
	/**
	 * Creates the jdbcType and entity objects for Apache Ignite configuration
	 * based on the @IgniteTable, @IgniteId and @IgniteColumn notations
	 * @param valueClass
	 * 					Class with the notations
	 * @throws Exception
	 * 					Message for possible notation errors or JdbcType or QueryEntity errors
	 */
	public static void loadConfiguration(Class<?> valueClass) throws Exception {
		// Process @IgniteTable
		if (valueClass.isAnnotationPresent(IgniteTable.class)) {
			Annotation annotationTable = valueClass.getAnnotation(IgniteTable.class);
			IgniteTable igniteTable = (IgniteTable) annotationTable;

			String cacheName = igniteTable.cacheName();
			// If Name not declared then use the class name
			String tableName = "".equals(igniteTable.name()) ? valueClass.getSimpleName() : igniteTable.name();

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

			fillTableSchema(fields, jdbcKeys, jdbcValues, entityKeys, entityFields);

			jdbcType.setKeyFields(jdbcKeys.toArray(new JdbcTypeField[jdbcKeys.size()]));
			jdbcType.setValueFields(jdbcValues.toArray(new JdbcTypeField[jdbcKeys.size()]));

			entity.setKeyFields(entityKeys);
			entity.setFields(entityFields);

			if (!isIdDeclared || !isColumnDeclared) {
				throw new Exception("[IgniteMapping] @IgniteTable should have @IgniteId and at least one @IgniteColumn");
			}
		}
	}

	/**
	 * Create the ignite objects related to the table and columns mapping defined by the notations
	 * @param fields
	 * @param jdbcKeys
	 * @param jdbcValues
	 * @param entityKeys
	 * @param entityFields
	 * @throws Exception
	 */
	private static void fillTableSchema(Field[] fields, List<JdbcTypeField> jdbcKeys, List<JdbcTypeField> jdbcValues,
			Set<String> entityKeys, LinkedHashMap<String, String> entityFields) throws Exception {
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
				Annotation annotationField = field.getAnnotation(IgniteColumn.class);
				IgniteColumn igniteField = (IgniteColumn) annotationField;

				isColumnDeclared = true;

				Class<?> type = field.getType();
				String name = field.getName();
				// If Name not declared then use the field name
				String columnName = "".equals(igniteField.name()) ? name : igniteField.name();

				// If is Id Key
				if (isKey) {
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
			}

			if (isKey && !isColumnDeclared) {
				throw new Exception("[IgniteMapping] @IgniteId should have also @IgniteColumn");
			}
		}
	}

	/**
	 * Get the SQL type for the main table types
	 * @param type
	 * @return
	 */
	private static int getSQLType(Class<?> type) {
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
}