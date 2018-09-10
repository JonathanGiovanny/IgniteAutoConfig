package com.ignite.utilities;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Id;

import com.ignite.utilities.annotations.IgniteColumn;
import com.ignite.utilities.annotations.IgniteId;
import com.ignite.utilities.annotations.IgniteTable;
import com.ignite.utilities.dto.ColumnDTO;
import com.ignite.utilities.dto.TableDTO;

public class ProcessAnnotationsDTO {

	private boolean isIdDeclared;
	private boolean isColumnDeclared;

	private static final String CLASSNAME = "[ProcessNotations]";

	/**
	 * Creates the jdbcType and entity objects for Apache Ignite configuration based on the @IgniteTable, @IgniteId and @IgniteColumn notations
	 * 
	 * @param valueClass
	 *            Class with the notations
	 * @throws Exception
	 *             Message for possible notation errors or JdbcType or QueryEntity errors
	 */
	public TableDTO loadClassData(Class<?> valueClass) throws Exception {
		TableDTO tableData = null;

		// Process @IgniteTable
		if (valueClass.isAnnotationPresent(IgniteTable.class)) {
			Annotation annotationTable = valueClass.getAnnotation(IgniteTable.class);
			IgniteTable igniteTable = (IgniteTable) annotationTable;

			String cacheName = igniteTable.cacheName();
			// If Name not declared then use the class name
			String tableName = "".equals(igniteTable.name()) ? valueClass.getSimpleName().toUpperCase() : igniteTable.name().toUpperCase();

			// Load table Data into DTO
			tableData = new TableDTO();
			tableData.setCacheName(cacheName);
			tableData.setTableName(tableName);
			tableData.setTableType(valueClass);

			Field[] fields = valueClass.getDeclaredFields();

			createTableSchema(tableData, fields);

			if (!isIdDeclared || !isColumnDeclared) {
				throw new Exception(CLASSNAME + " [loadData] @IgniteTable should have @IgniteId and at least one @IgniteColumn");
			}
		}

		return tableData;
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
	private void createTableSchema(TableDTO tableData, Field[] fields) throws Exception {
		// Load column Data into DTO
		List<ColumnDTO> columns = new ArrayList<>();

		// Process Fields
		for (Field field : fields) {
			boolean isKey = false;

			// If field is annotated with @IgniteId or javax persistence Id
			if (field.isAnnotationPresent(IgniteId.class) || field.isAnnotationPresent(Id.class)) {
				isIdDeclared = true;
				isKey = true;
			}

			// If field is annotated with @IgniteColumn or javax persistence Column
			if (field.isAnnotationPresent(IgniteColumn.class) || field.isAnnotationPresent(Column.class)) {
				ColumnDTO columnData = new ColumnDTO();

				Class<?> type = field.getType();
				String name = field.getName();
				
				String columnName = null;

				Annotation annotationField = field.getAnnotation(IgniteColumn.class);
				if (annotationField == null) {
					annotationField = field.getAnnotation(Column.class);
					Column igniteField = (Column) annotationField;
					// If Name not declared then use the field name
					columnName = igniteField == null || "".equals(igniteField.name()) ? name : igniteField.name();

				} else {
					IgniteColumn igniteField = (IgniteColumn) annotationField;
					columnName = igniteField == null || "".equals(igniteField.name()) ? name : igniteField.name();
				}

				isColumnDeclared = true;


				columnData.setColumnName(columnName);
				columnData.setFieldName(name);
				columnData.setFieldType(type);
				// If is Id Key
				columnData.setKey(isKey);

				columns.add(columnData);
			}

			if (isKey && !isColumnDeclared) {
				throw new Exception(CLASSNAME + "[createTableSchema] @IgniteId should have also @IgniteColumn");
			}
		}

		// Fill columns info for tableData
		tableData.setColumns(columns);
	}
}
