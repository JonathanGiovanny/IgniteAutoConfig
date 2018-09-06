package com.ignite.utilities.dto;

import java.io.Serializable;
import java.util.List;

import lombok.Data;

@Data
public class TableDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1450886151145546940L;

	private String tableName;
	private String cacheName;
	private Class<?> tableType;
	private List<ColumnDTO> columns;
}
