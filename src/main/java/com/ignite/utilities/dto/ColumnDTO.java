package com.ignite.utilities.dto;

import java.io.Serializable;

import lombok.Data;

@Data
public class ColumnDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -782538290213794813L;

	private boolean isKey;
	private String columnName;
	private String fieldName;
	private Class<?> fieldType;
}
