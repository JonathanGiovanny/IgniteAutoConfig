package com.ignite.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
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
	private static List<String> cacheNames = new ArrayList<>();

	/** List of the original classes used to map the schema */
	private static List<Class<?>> classes = new ArrayList<>();

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
	 * Get the created JDBCType data based on the notations <br>
	 * <strong>* It shoud have classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static JdbcType[] getJDBCTypes() throws Exception {
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
		Map<String, CacheConfiguration<?, ?>> cacheConfigs = new HashMap<>();

		try {
			for (String cacheName : cacheNames) {
				List<TableDTO> tablesPerCache = cacheTables.get(cacheName);

				// Generate a cacheConfiguration per cacheName
				CacheConfiguration<?, ?> cacheConfig = new CacheConfiguration<>();
				cacheConfig.setReadThrough(true);
				cacheConfig.setWriteThrough(true);
				cacheConfig.setWriteBehindEnabled(true);
				cacheConfig.setWriteBehindFlushFrequency(250);

				cacheConfig.setName(cacheName);
				cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);
				cacheConfig.setBackups(0);

				Collection<QueryEntity> queryEntities = new ArrayList<>();

				// One store factory per cacheConfig with the data of each Table
				CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
				storeFactory.setDataSourceFactory(dataSource);
				storeFactory.setDialect(dialect);

				for (TableDTO tableData : tablesPerCache) {
					// Generates the JdbcType data and QueryEntity for the table
					GenerateMapping gm = new GenerateMapping();
					gm.createTableSchema(tableData.getCacheName(), tableData);

					storeFactory.setTypes(gm.getJdbcType());

					queryEntities.add(gm.getQueryEntity());
				}

				cacheConfig.setCacheStoreFactory(storeFactory);
				cacheConfig.setQueryEntities(queryEntities);

				// Add the cacheConfiguration created to a list
				cacheConfigs.put(cacheName, cacheConfig);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return cacheConfigs;
	}
}
