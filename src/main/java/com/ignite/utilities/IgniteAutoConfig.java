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
 * * The purpose of this project is to simplify the configuration of the Ignite lib through a few calls and using annotations as
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
	private static List<String> cacheNames = new ArrayList<>();

	/** List of the original classes used to map the schema */
	private static List<Class<?>> classes = new ArrayList<>();

	/** Will define if the configuration is based on the CacheStore object */
	private static boolean isCacheStore;

	/**
	 * Add classes to be processed by
	 * 
	 * @param classToAdd
	 * @throws Exception
	 */
	public static void addClass(Class<?> classToAdd) throws Exception {
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
	 * Generate the array with the cache Configuration objects for each cache name mapped on each table.<br>
	 * <br>
	 * To use it:<br>
	 * <ol>
	 * <li>Add each class with the <code>addClass(Class<?>)</code> method</li>
	 * <li>Call this method (<code>generateCacheConfiguration(...)</code>)</li>
	 * <li>Set it to the IgniteConfiguration</li>
	 * <li>Done c:</li>
	 * </ol>
	 * 
	 * @param dataSource
	 * @param dialect
	 * @return Map < cacheName, CacheConfiguration>
	 */
	public static Map<String, CacheConfiguration<?, ?>> generateCacheConfiguration(Factory<DataSource> dataSource, JdbcDialect dialect) {
		GenerateCacheConfiguration.init(isCacheStore, cacheTables, cacheNames);
		return GenerateCacheConfiguration.generateCacheConfiguration(dataSource, dialect);
	}

	/**
	 * Get the created JDBCType data based on the notations <br>
	 * <strong>* It should have classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static JdbcType[] getJDBCTypes() throws Exception {
		return jdbcTypes;
	}

	/**
	 * Get the created query entity based on the notations <br>
	 * <strong>* It should have classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static Collection<QueryEntity> getQueryEntities() throws Exception {
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
	 * Set if a cacheStore will be generated per table
	 * @return
	 */
	public static boolean isCacheStore() {
		return isCacheStore;
	}

	/**
	 * Get the value of the variable
	 * @param isCacheStore
	 */
	public static void setCacheStore(boolean isCacheStore) {
		IgniteAutoConfig.isCacheStore = isCacheStore;
	}
}
