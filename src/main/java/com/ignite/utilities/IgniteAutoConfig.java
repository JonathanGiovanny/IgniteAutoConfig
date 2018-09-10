package com.ignite.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.sql.DataSource;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;

import com.ignite.utilities.dto.TableDTO;

/**
 * Class used to generate the Apache Ignite configuration set.<br>
 * <br>
 * * The porpuse of this project is to simplify the configuration of the Ignite lib through a few calls and using annotations as
 * <strong>@IgniteTable</strong>, <strong>@IgniteId</strong> and/or <strong>@IgniteColumn</strong> the last two are optional only if the mapped table
 * is using the javax.persistence lib which should add <strong>@Id</strong> and <strong>@Column</strong>.
 * 
 * @author jcamargos
 * @date 10/09/2018
 */
public class IgniteAutoConfig {

	private static JdbcType[] jdbcTypes;
	private static Collection<QueryEntity> entities;

	/** Map Key: cacheName, Value: Tables with that cache */
	private static Map<String, List<TableDTO>> cacheTables = new HashMap<>();

	/** List of the caches created */
	private static List<String> cacheNames;

	/** List of the original classes used to map the schema */
	private static List<Class<?>> classes;

	@SuppressWarnings("unused")
	private static final String CLASSNAME = "[IgniteAutoConfig]";

	/**
	 * Add classes to be processed by
	 * 
	 * @param classToAdd
	 * @throws Exception
	 */
	public static void addClass(Class<?> classToAdd) throws Exception {
		if (classes == null) {
			classes = new ArrayList<>();
		}

		// Generates the TableDTO with the info of the class added to be mapped
		ProcessAnnotationsDTO pa = new ProcessAnnotationsDTO();
		TableDTO tableMapped = pa.loadClassData(classToAdd);

		List<TableDTO> tablesIgnite = null;
		// Verify if the cache name used already exists to add the class to the cache being used to that
		if (cacheTables.get(tableMapped.getCacheName()) == null) {
			tablesIgnite = new ArrayList<>();
		} else {
			tablesIgnite = cacheTables.get(tableMapped.getCacheName());
		}

		tablesIgnite.add(tableMapped);
		cacheTables.put(tableMapped.getCacheName(), tablesIgnite);
		cacheNames.add(tableMapped.getCacheName());
		classes.add(classToAdd);
	}

	/**
	 * Fill the arrays with the data based on the classes added
	 * 
	 * @throws Exception
	 */
	private static void processNotations() throws Exception {
		jdbcTypes = new JdbcType[classes.size()];
		entities = new ArrayList<>();
		cacheNames = new ArrayList<>();

		for (int i = 0; i < jdbcTypes.length; i++) {
			Class<?> igniteClass = classes.get(i);
			ProcessAnnotations pa = new ProcessAnnotations();
			pa.loadData(igniteClass);
			jdbcTypes[i] = pa.getJDBCType();
			entities.add(pa.getQueryEntity());
			cacheNames.add(pa.getCacheName());
		}
	}

	/**
	 * Get the created JDBCType data based on the notations <br>
	 * <strong>* It shoud have classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static JdbcType[] getJDBCTypes() throws Exception {
		if (jdbcTypes == null) {
			processNotations();
		}

		return jdbcTypes;
	}

	/**
	 * Get the created query entity based on the notations <br>
	 * <strong>* It shoud have classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static Collection<QueryEntity> getQueryEntities() throws Exception {
		if (entities == null) {
			processNotations();
		}

		return entities;
	}

	/**
	 * Get the list of cacheNames mapped
	 * 
	 * @return
	 */
	public static List<String> getCacheNames() {
		return cacheNames;
	}

	/**
	 * Generate the array with the cache Configuration objects for each cache name mapped on each table
	 * 
	 * @param dataSource
	 * @param dialect
	 * @return
	 */
	public static CacheConfiguration<?, ?>[] generateCacheConfiguration(Factory<DataSource> dataSource, JdbcDialect dialect) {
		return null;
	}
}
